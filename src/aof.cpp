#include "aof.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kv {

namespace {

constexpr std::array<uint32_t, 256> make_crc_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        table[i] = c;
    }
    return table;
}

constexpr auto kCrcTable = make_crc_table();

constexpr uint32_t kMaxFieldLen = 1u << 30;
constexpr size_t kHeaderLen = 1 + 4;

void put_u32(std::string& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t get_u32(const unsigned char* p) {
    return uint32_t{p[0]} | uint32_t{p[1]} << 8 | uint32_t{p[2]} << 16 | uint32_t{p[3]} << 24;
}

[[noreturn]] void throw_errno(int err, const std::string& what) {
    throw std::system_error(err, std::generic_category(), what);
}

}

uint32_t crc32(const void* data, size_t len, uint32_t crc) {
    const auto* p = static_cast<const unsigned char*>(data);
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) crc = kCrcTable[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

std::optional<FsyncPolicy> parse_fsync_policy(std::string_view name) {
    if (name == "always") return FsyncPolicy::Always;
    if (name == "everysec") return FsyncPolicy::EverySec;
    if (name == "no") return FsyncPolicy::No;
    return std::nullopt;
}

const char* to_string(FsyncPolicy policy) {
    switch (policy) {
        case FsyncPolicy::Always: return "always";
        case FsyncPolicy::EverySec: return "everysec";
        case FsyncPolicy::No: return "no";
    }
    return "?";
}

std::string encode_record(LogOp op, std::string_view key, std::string_view value) {
    if (key.size() > kMaxFieldLen || value.size() > kMaxFieldLen) {
        throw std::length_error("key or value too large for a log record");
    }
    std::string rec;
    rec.reserve(kHeaderLen + key.size() + 4 + value.size() + 4);
    rec.push_back(static_cast<char>(op));
    put_u32(rec, static_cast<uint32_t>(key.size()));
    rec.append(key);
    put_u32(rec, static_cast<uint32_t>(value.size()));
    rec.append(value);
    put_u32(rec, crc32(rec.data(), rec.size()));
    return rec;
}

ReplayStats Aof::replay(const std::string& path, const ApplyFn& apply) {
    namespace fs = std::filesystem;
    ReplayStats stats;
    if (!fs::exists(path)) return stats;

    const uint64_t file_size = fs::file_size(path);
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + path + " for replay");

    uint64_t pos = 0;
    auto read_exact = [&](void* dst, uint64_t n) {
        if (file_size - pos < n) return false;
        in.read(static_cast<char*>(dst), static_cast<std::streamsize>(n));
        if (static_cast<uint64_t>(in.gcount()) != n) return false;
        pos += n;
        return true;
    };

    std::string key;
    std::string value;
    while (pos < file_size) {
        unsigned char header[kHeaderLen];
        unsigned char vlen_buf[4];
        unsigned char crc_buf[4];

        if (!read_exact(header, sizeof header)) break;
        const uint8_t op = header[0];
        if (op != static_cast<uint8_t>(LogOp::Set) && op != static_cast<uint8_t>(LogOp::Del)) break;

        const uint32_t klen = get_u32(header + 1);
        if (klen > kMaxFieldLen || file_size - pos < klen) break;
        key.resize(klen);
        if (!read_exact(key.data(), klen)) break;

        if (!read_exact(vlen_buf, sizeof vlen_buf)) break;
        const uint32_t vlen = get_u32(vlen_buf);
        if (vlen > kMaxFieldLen || file_size - pos < vlen) break;
        value.resize(vlen);
        if (!read_exact(value.data(), vlen)) break;

        if (!read_exact(crc_buf, sizeof crc_buf)) break;
        uint32_t crc = crc32(header, sizeof header);
        crc = crc32(key.data(), key.size(), crc);
        crc = crc32(vlen_buf, sizeof vlen_buf, crc);
        crc = crc32(value.data(), value.size(), crc);
        if (crc != get_u32(crc_buf)) break;

        apply(static_cast<LogOp>(op), key, value);
        ++stats.records;
        stats.valid_bytes = pos;
    }
    in.close();

    if (stats.valid_bytes < file_size) {
        fs::resize_file(path, stats.valid_bytes);
        stats.truncated_bytes = file_size - stats.valid_bytes;
    }
    return stats;
}

Aof::Aof(std::string path, FsyncPolicy policy) : path_(std::move(path)), policy_(policy) {
    int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) throw_errno(errno, "open " + path_);
    fd_.reset(fd);

    struct stat st {};
    if (::fstat(fd_.get(), &st) != 0) throw_errno(errno, "fstat " + path_);
    file_size_ = static_cast<uint64_t>(st.st_size);

    if (policy_ == FsyncPolicy::EverySec) flusher_ = std::thread([this] { flusher_loop(); });
}

Aof::~Aof() {
    {
        std::lock_guard lock(flush_mu_);
        stopping_ = true;
    }
    flush_cv_.notify_all();
    if (flusher_.joinable()) flusher_.join();
    try {
        sync();
    } catch (const std::exception& e) {
        std::cerr << "aof: final fsync failed: " << e.what() << '\n';
    }
}

uint64_t Aof::append(LogOp op, std::string_view key, std::string_view value) {
    const std::string rec = encode_record(op, key, value);

    std::lock_guard lock(write_mu_);
    if (broken_) throw std::runtime_error("log unwritable after an earlier I/O error");
    if (!write_all(fd_.get(), rec)) {
        const int err = errno;
        if (::ftruncate(fd_.get(), static_cast<off_t>(file_size_)) != 0) broken_ = true;
        throw_errno(err, "append to " + path_);
    }
    file_size_ += rec.size();
    const uint64_t seq = ++last_seq_;
    written_seq_.store(seq, std::memory_order_release);
    return seq;
}

void Aof::sync_locked() {
    const uint64_t target = written_seq_.load(std::memory_order_acquire);
    if (target <= synced_seq_.load(std::memory_order_relaxed)) return;
    if (::fdatasync(fd_.get()) != 0) throw_errno(errno, "fdatasync " + path_);
    synced_seq_.store(target, std::memory_order_release);
}

void Aof::sync() {
    std::lock_guard lock(sync_mu_);
    sync_locked();
}

void Aof::wait_durable(uint64_t seq) {
    if (policy_ != FsyncPolicy::Always || seq == 0) return;
    if (synced_seq_.load(std::memory_order_acquire) >= seq) return;
    std::lock_guard lock(sync_mu_);
    if (synced_seq_.load(std::memory_order_acquire) >= seq) return;
    sync_locked();
}

void Aof::flusher_loop() {
    std::unique_lock lock(flush_mu_);
    while (!flush_cv_.wait_for(lock, std::chrono::seconds(1), [this] { return stopping_; })) {
        lock.unlock();
        try {
            sync();
        } catch (const std::exception& e) {
            std::cerr << "aof: background fsync failed: " << e.what() << '\n';
        }
        lock.lock();
    }
}

}
