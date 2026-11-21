#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kv {

class Aof;

struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};

class Store {
public:
    explicit Store(size_t num_shards = 16);

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    std::optional<std::string> get(std::string_view key) const;

    bool append_value(std::string_view key, std::string& out) const;

    void set(std::string_view key, std::string_view value, uint64_t* log_seq = nullptr);
    bool del(std::string_view key, uint64_t* log_seq = nullptr);

    size_t size() const;
    size_t num_shards() const noexcept { return num_shards_; }

    void attach_log(Aof* log) noexcept { log_ = log; }

private:
    struct alignas(64) Shard {
        std::shared_mutex mu;
        std::unordered_map<std::string, std::string, StringHash, std::equal_to<>> map;
    };

    Shard& shard_for(std::string_view key) const;

    size_t num_shards_;
    std::unique_ptr<Shard[]> shards_;
    Aof* log_ = nullptr;
};

}
