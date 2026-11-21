#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include "fd.hpp"

namespace kv {

enum class FsyncPolicy { Always, EverySec, No };

std::optional<FsyncPolicy> parse_fsync_policy(std::string_view name);
const char* to_string(FsyncPolicy policy);

enum class LogOp : uint8_t { Set = 1, Del = 2 };

uint32_t crc32(const void* data, size_t len, uint32_t crc = 0);

std::string encode_record(LogOp op, std::string_view key, std::string_view value);

struct ReplayStats {
    uint64_t records = 0;
    uint64_t valid_bytes = 0;
    uint64_t truncated_bytes = 0;
};

class Aof {
public:
    using ApplyFn = std::function<void(LogOp op, std::string_view key, std::string_view value)>;

    static ReplayStats replay(const std::string& path, const ApplyFn& apply);

    Aof(std::string path, FsyncPolicy policy);
    ~Aof();

    Aof(const Aof&) = delete;
    Aof& operator=(const Aof&) = delete;

    uint64_t append(LogOp op, std::string_view key, std::string_view value);

    void wait_durable(uint64_t seq);

    void sync();

    FsyncPolicy policy() const noexcept { return policy_; }
    const std::string& path() const noexcept { return path_; }

private:
    void sync_locked();
    void flusher_loop();

    const std::string path_;
    const FsyncPolicy policy_;
    Fd fd_;

    std::mutex write_mu_;
    uint64_t file_size_ = 0;
    uint64_t last_seq_ = 0;
    bool broken_ = false;
    std::atomic<uint64_t> written_seq_{0};

    std::mutex sync_mu_;
    std::atomic<uint64_t> synced_seq_{0};

    std::mutex flush_mu_;
    std::condition_variable flush_cv_;
    bool stopping_ = false;
    std::thread flusher_;
};

}
