#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "store.hpp"

using kv::Store;

namespace {

std::string get_or_missing(const Store& s, std::string_view key) {
    return s.get(key).value_or("<missing>");
}

}

TEST(Store, GetMissingKey) {
    Store s;
    EXPECT_FALSE(s.get("nope").has_value());
}

TEST(Store, SetThenGet) {
    Store s;
    s.set("a", "1");
    EXPECT_EQ(get_or_missing(s, "a"), "1");
}

TEST(Store, SetOverwrites) {
    Store s;
    s.set("a", "1");
    s.set("a", "2");
    EXPECT_EQ(get_or_missing(s, "a"), "2");
    EXPECT_EQ(s.size(), 1u);
}

TEST(Store, DelExistingThenMissing) {
    Store s;
    s.set("a", "1");
    EXPECT_TRUE(s.del("a"));
    EXPECT_FALSE(s.get("a").has_value());
    EXPECT_FALSE(s.del("a"));
}

TEST(Store, AppendValue) {
    Store s;
    s.set("k", "world");
    std::string out = "hello ";
    EXPECT_TRUE(s.append_value("k", out));
    EXPECT_EQ(out, "hello world");
    EXPECT_FALSE(s.append_value("missing", out));
    EXPECT_EQ(out, "hello world");
}

TEST(Store, SizeAcrossShards) {
    Store s(16);
    for (int i = 0; i < 1000; ++i) s.set("key" + std::to_string(i), std::to_string(i));
    EXPECT_EQ(s.size(), 1000u);
    for (int i = 0; i < 500; ++i) EXPECT_TRUE(s.del("key" + std::to_string(i)));
    EXPECT_EQ(s.size(), 500u);
    EXPECT_EQ(get_or_missing(s, "key999"), "999");
}

TEST(Store, SingleShard) {
    Store s(1);
    for (int i = 0; i < 100; ++i) s.set("k" + std::to_string(i), "v");
    EXPECT_EQ(s.size(), 100u);
    EXPECT_EQ(s.num_shards(), 1u);
}

TEST(Store, ZeroShardsRejected) {
    EXPECT_THROW(Store(0), std::invalid_argument);
}

TEST(Store, VeryLongValue) {
    Store s;
    const std::string big(4 * 1024 * 1024, 'z');
    s.set("big", big);
    EXPECT_EQ(get_or_missing(s, "big"), big);
}

TEST(Store, ConcurrentWritersOnDistinctKeys) {
    Store s;
    constexpr int kThreads = 8;
    constexpr int kPerThread = 2000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&s, t] {
            for (int i = 0; i < kPerThread; ++i) {
                s.set("t" + std::to_string(t) + ":" + std::to_string(i), std::to_string(i));
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(s.size(), static_cast<size_t>(kThreads * kPerThread));
    EXPECT_EQ(get_or_missing(s, "t3:1234"), "1234");
}

TEST(Store, ConcurrentReadersAndWritersOnHotKeys) {
    Store s(4);
    std::atomic<bool> saw_garbage{false};
    std::vector<std::thread> threads;
    for (int w = 0; w < 4; ++w) {
        threads.emplace_back([&s, w] {
            for (int i = 0; i < 5000; ++i) {
                const std::string key = "hot" + std::to_string(i % 8);
                if (i % 5 == 0) s.del(key);
                else s.set(key, "w" + std::to_string(w) + "-" + std::to_string(i));
            }
        });
    }
    for (int r = 0; r < 4; ++r) {
        threads.emplace_back([&s, &saw_garbage] {
            for (int i = 0; i < 5000; ++i) {
                auto v = s.get("hot" + std::to_string(i % 8));
                if (v && (v->empty() || (*v)[0] != 'w')) saw_garbage = true;
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_FALSE(saw_garbage.load());
}
