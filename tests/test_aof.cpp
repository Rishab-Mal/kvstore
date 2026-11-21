#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "aof.hpp"
#include "store.hpp"

namespace fs = std::filesystem;
using kv::Aof;
using kv::FsyncPolicy;
using kv::LogOp;
using kv::Store;

namespace {

struct TempFile {
    fs::path path;
    TempFile() {
        static std::atomic<int> counter{0};
        path = fs::temp_directory_path() /
               ("kv_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter++) + ".aof");
        fs::remove(path);
    }
    ~TempFile() {
        std::error_code ec;
        fs::remove(path, ec);
    }
    std::string str() const { return path.string(); }
};

kv::ReplayStats replay_into(Store& store, const std::string& path) {
    return Aof::replay(path, [&](LogOp op, std::string_view key, std::string_view value) {
        if (op == LogOp::Set) store.set(key, value);
        else store.del(key);
    });
}

void write_file(const fs::path& p, const std::string& data) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

std::string get_or_missing(const Store& s, std::string_view key) {
    return s.get(key).value_or("<missing>");
}

}

TEST(Crc32, KnownVector) {
    EXPECT_EQ(kv::crc32("123456789", 9), 0xCBF43926u);
}

TEST(Crc32, Chaining) {
    EXPECT_EQ(kv::crc32("56789", 5, kv::crc32("1234", 4)), 0xCBF43926u);
}

TEST(Aof, ParseFsyncPolicy) {
    EXPECT_TRUE(kv::parse_fsync_policy("always") == FsyncPolicy::Always);
    EXPECT_TRUE(kv::parse_fsync_policy("everysec") == FsyncPolicy::EverySec);
    EXPECT_TRUE(kv::parse_fsync_policy("no") == FsyncPolicy::No);
    EXPECT_FALSE(kv::parse_fsync_policy("sometimes").has_value());
}

TEST(Aof, MissingFileReplaysNothing) {
    TempFile f;
    Store s;
    const auto stats = replay_into(s, f.str());
    EXPECT_EQ(stats.records, 0u);
    EXPECT_EQ(s.size(), 0u);
}

TEST(Aof, RoundTripThroughStore) {
    TempFile f;
    {
        Store s;
        Aof log(f.str(), FsyncPolicy::No);
        s.attach_log(&log);
        s.set("a", "1");
        s.set("b", "2");
        s.set("a", "3");
        s.del("b");
        s.del("never-existed");
    }
    Store r;
    const auto stats = replay_into(r, f.str());
    EXPECT_EQ(stats.records, 4u);
    EXPECT_EQ(stats.truncated_bytes, 0u);
    EXPECT_EQ(get_or_missing(r, "a"), "3");
    EXPECT_FALSE(r.get("b").has_value());
    EXPECT_EQ(r.size(), 1u);
}

TEST(Aof, ValuesAreBinarySafe) {
    TempFile f;
    using namespace std::string_literals;
    const std::string tricky = "line1\nline2 with spaces\r\n\0after-nul"s;
    EXPECT_EQ(tricky.size(), 35u);
    {
        Store s;
        Aof log(f.str(), FsyncPolicy::No);
        s.attach_log(&log);
        s.set("tricky", tricky);
        s.set("empty", "");
    }
    Store r;
    replay_into(r, f.str());
    EXPECT_EQ(get_or_missing(r, "tricky"), tricky);
    EXPECT_EQ(get_or_missing(r, "empty"), "");
}

TEST(Aof, TornTailIsDetectedAndTruncated) {
    TempFile f;
    {
        Aof log(f.str(), FsyncPolicy::No);
        log.append(LogOp::Set, "a", "1");
        log.append(LogOp::Set, "b", "2");
        log.append(LogOp::Set, "c", "3");
    }
    const auto good_size = fs::file_size(f.path);

    const std::string rec = kv::encode_record(LogOp::Set, "d", "4444444444");
    {
        std::ofstream out(f.path, std::ios::binary | std::ios::app);
        out.write(rec.data(), static_cast<std::streamsize>(rec.size() / 2));
    }

    Store r;
    const auto stats = replay_into(r, f.str());
    EXPECT_EQ(stats.records, 3u);
    EXPECT_EQ(stats.truncated_bytes, rec.size() / 2);
    EXPECT_EQ(fs::file_size(f.path), good_size);
    EXPECT_EQ(get_or_missing(r, "c"), "3");
    EXPECT_FALSE(r.get("d").has_value());

    {
        Aof log(f.str(), FsyncPolicy::No);
        log.append(LogOp::Set, "e", "5");
    }
    Store r2;
    EXPECT_EQ(replay_into(r2, f.str()).records, 4u);
    EXPECT_EQ(get_or_missing(r2, "e"), "5");
}

TEST(Aof, BadChecksumStopsReplay) {
    TempFile f;
    const std::string r1 = kv::encode_record(LogOp::Set, "a", "1");
    const std::string r2 = kv::encode_record(LogOp::Set, "b", "2");
    const std::string r3 = kv::encode_record(LogOp::Set, "c", "3");
    std::string data = r1 + r2 + r3;
    data[r1.size() + 5] ^= 0x01;
    write_file(f.path, data);

    Store r;
    const auto stats = replay_into(r, f.str());
    EXPECT_EQ(stats.records, 1u);
    EXPECT_EQ(fs::file_size(f.path), r1.size());
    EXPECT_EQ(get_or_missing(r, "a"), "1");
    EXPECT_FALSE(r.get("c").has_value());
}

TEST(Aof, GarbageLengthDoesNotAllocate) {
    TempFile f;
    std::string data = kv::encode_record(LogOp::Set, "a", "1");
    data += std::string("\x01\xff\xff\xff\x3f", 5);
    write_file(f.path, data);
    Store r;
    EXPECT_EQ(replay_into(r, f.str()).records, 1u);
}

TEST(Aof, SequenceNumbersAndDurableWait) {
    TempFile f;
    Aof log(f.str(), FsyncPolicy::Always);
    const uint64_t a = log.append(LogOp::Set, "a", "1");
    const uint64_t b = log.append(LogOp::Set, "b", "2");
    EXPECT_LT(a, b);
    EXPECT_NO_THROW(log.wait_durable(b));
    EXPECT_NO_THROW(log.wait_durable(a));
}

TEST(Aof, ConcurrentWritesReplayToSameState) {
    TempFile f;
    constexpr int kKeys = 32;
    std::vector<std::string> expected(kKeys);
    {
        Store s(8);
        Aof log(f.str(), FsyncPolicy::Always);
        s.attach_log(&log);
        std::vector<std::thread> threads;
        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([&s, &log, t] {
                for (int i = 0; i < 300; ++i) {
                    const std::string key = "k" + std::to_string((i * 7 + t) % kKeys);
                    uint64_t seq = 0;
                    if (i % 11 == 0) s.del(key, &seq);
                    else s.set(key, "t" + std::to_string(t) + "-" + std::to_string(i), &seq);
                    log.wait_durable(seq);
                }
            });
        }
        for (auto& th : threads) th.join();
        for (int k = 0; k < kKeys; ++k) expected[k] = get_or_missing(s, "k" + std::to_string(k));
    }
    Store r;
    replay_into(r, f.str());
    for (int k = 0; k < kKeys; ++k) EXPECT_EQ(get_or_missing(r, "k" + std::to_string(k)), expected[k]);
}
