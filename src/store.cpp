#include "store.hpp"

#include <mutex>
#include <stdexcept>

#include "aof.hpp"

namespace kv {

namespace {

inline uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

}

Store::Store(size_t num_shards) : num_shards_(num_shards) {
    if (num_shards_ == 0) throw std::invalid_argument("Store needs at least one shard");
    shards_ = std::make_unique<Shard[]>(num_shards_);
}

Store::Shard& Store::shard_for(std::string_view key) const {
    return shards_[mix(StringHash{}(key)) % num_shards_];
}

std::optional<std::string> Store::get(std::string_view key) const {
    Shard& s = shard_for(key);
    std::shared_lock lock(s.mu);
    auto it = s.map.find(key);
    if (it == s.map.end()) return std::nullopt;
    return it->second;
}

bool Store::append_value(std::string_view key, std::string& out) const {
    Shard& s = shard_for(key);
    std::shared_lock lock(s.mu);
    auto it = s.map.find(key);
    if (it == s.map.end()) return false;
    out.append(it->second);
    return true;
}

void Store::set(std::string_view key, std::string_view value, uint64_t* log_seq) {
    Shard& s = shard_for(key);
    std::unique_lock lock(s.mu);
    const uint64_t seq = log_ ? log_->append(LogOp::Set, key, value) : 0;
    auto it = s.map.find(key);
    if (it != s.map.end()) {
        it->second.assign(value.data(), value.size());
    } else {
        s.map.emplace(std::string(key), std::string(value));
    }
    if (log_seq) *log_seq = seq;
}

bool Store::del(std::string_view key, uint64_t* log_seq) {
    Shard& s = shard_for(key);
    std::unique_lock lock(s.mu);
    auto it = s.map.find(key);
    if (it == s.map.end()) {
        if (log_seq) *log_seq = 0;
        return false;
    }
    const uint64_t seq = log_ ? log_->append(LogOp::Del, key, {}) : 0;
    s.map.erase(it);
    if (log_seq) *log_seq = seq;
    return true;
}

size_t Store::size() const {
    size_t total = 0;
    for (size_t i = 0; i < num_shards_; ++i) {
        std::shared_lock lock(shards_[i].mu);
        total += shards_[i].map.size();
    }
    return total;
}

}
