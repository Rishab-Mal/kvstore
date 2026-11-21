#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <latch>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "fd.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::string host = "127.0.0.1";
    uint16_t port = 6380;
    size_t clients = 8;
    size_t requests = 100000;
    double read_ratio = 0.9;
    size_t keys = 100000;
    size_t value_size = 64;
    size_t pipeline = 1;
    bool preload = true;
    uint64_t seed = 1;
};

void usage(const char* argv0) {
    std::cerr << "usage: " << argv0 << " [options]\n"
              << "  --host ADDR        server IPv4 address (default 127.0.0.1)\n"
              << "  --port N           server port (default 6380)\n"
              << "  --clients C        concurrent connections, one thread each (default 8)\n"
              << "  --requests N       requests per client (default 100000)\n"
              << "  --read-ratio R     fraction of GETs, 0..1 (default 0.9)\n"
              << "  --keys K           key space size (default 100000)\n"
              << "  --value-size B     bytes per value (default 64)\n"
              << "  --pipeline P       requests sent per round trip (default 1)\n"
              << "  --no-preload       skip writing every key before the timed run\n"
              << "  --seed S           RNG seed (default 1)\n";
}

template <class T>
bool parse_number(std::string_view s, T& out) {
    auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && end == s.data() + s.size();
}

std::optional<Options> parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return std::nullopt;
        }
        if (arg == "--no-preload") {
            o.preload = false;
            continue;
        }
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << arg << '\n';
            return std::nullopt;
        }
        const std::string_view v = argv[++i];
        bool ok = true;
        if (arg == "--host") o.host = std::string(v);
        else if (arg == "--port") ok = parse_number(v, o.port);
        else if (arg == "--clients") ok = parse_number(v, o.clients) && o.clients > 0;
        else if (arg == "--requests") ok = parse_number(v, o.requests) && o.requests > 0;
        else if (arg == "--keys") ok = parse_number(v, o.keys) && o.keys > 0;
        else if (arg == "--value-size") ok = parse_number(v, o.value_size) && o.value_size > 0;
        else if (arg == "--pipeline") ok = parse_number(v, o.pipeline) && o.pipeline > 0;
        else if (arg == "--seed") ok = parse_number(v, o.seed);
        else if (arg == "--read-ratio") {
            try {
                o.read_ratio = std::stod(std::string(v));
            } catch (...) {
                ok = false;
            }
            ok = ok && o.read_ratio >= 0.0 && o.read_ratio <= 1.0;
        } else {
            std::cerr << "unknown option: " << arg << '\n';
            usage(argv[0]);
            return std::nullopt;
        }
        if (!ok) {
            std::cerr << "invalid value for " << arg << ": " << v << '\n';
            return std::nullopt;
        }
    }
    return o;
}

class Connection {
public:
    bool connect(const std::string& host, uint16_t port) {
        fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
        if (!fd_.valid()) return false;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
            errno = EINVAL;
            return false;
        }
        if (::connect(fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) return false;
        int one = 1;
        ::setsockopt(fd_.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        return true;
    }

    bool send(std::string_view data) { return kv::send_all(fd_.get(), data); }

    template <class F>
    bool read_lines(size_t count, F&& on_line) {
        size_t start = 0;
        while (count > 0) {
            const size_t nl = buf_.find('\n', start);
            if (nl == std::string::npos) {
                buf_.erase(0, start);
                start = 0;
                char chunk[64 * 1024];
                const ssize_t n = ::recv(fd_.get(), chunk, sizeof chunk, 0);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return false;
                buf_.append(chunk, static_cast<size_t>(n));
                continue;
            }
            on_line(std::string_view(buf_.data() + start, nl - start));
            start = nl + 1;
            --count;
        }
        buf_.erase(0, start);
        return true;
    }

private:
    kv::Fd fd_;
    std::string buf_;
};

struct ClientResult {
    std::vector<uint64_t> latencies_ns;
    uint64_t errors = 0;
    uint64_t misses = 0;
    std::string failure;
};

void append_key(std::string& out, size_t k) {
    char digits[24];
    auto [end, ec] = std::to_chars(digits, digits + sizeof digits, k);
    out += "key:";
    out.append(digits, end);
}

bool preload(Connection& conn, const Options& o, size_t idx, const std::string& value) {
    const size_t lo = idx * o.keys / o.clients;
    const size_t hi = (idx + 1) * o.keys / o.clients;
    std::string batch;
    for (size_t k = lo; k < hi;) {
        batch.clear();
        size_t n = 0;
        for (; k < hi && n < 256; ++k, ++n) {
            batch += "SET ";
            append_key(batch, k);
            batch += ' ';
            batch += value;
            batch += '\n';
        }
        if (!conn.send(batch) || !conn.read_lines(n, [](std::string_view) {})) return false;
    }
    return true;
}

void run_client(const Options& o, size_t idx, std::latch& start_line, ClientResult& res) {
    const std::string value(o.value_size, 'x');
    Connection conn;
    bool ok = conn.connect(o.host, o.port);
    if (!ok) res.failure = std::string("connect: ") + std::strerror(errno);
    if (ok && o.preload && !preload(conn, o, idx, value)) {
        ok = false;
        res.failure = "connection lost during preload";
    }
    if (!ok) {
        start_line.count_down();
        return;
    }

    std::mt19937_64 rng(o.seed * 1000003 + idx);
    std::uniform_int_distribution<size_t> pick_key(0, o.keys - 1);
    std::uniform_real_distribution<double> coin(0.0, 1.0);
    res.latencies_ns.reserve(o.requests);
    std::string batch;

    start_line.arrive_and_wait();

    for (size_t done = 0; done < o.requests;) {
        const size_t n = std::min(o.pipeline, o.requests - done);
        batch.clear();
        for (size_t i = 0; i < n; ++i) {
            const bool is_read = coin(rng) < o.read_ratio;
            batch += is_read ? "GET " : "SET ";
            append_key(batch, pick_key(rng));
            if (!is_read) {
                batch += ' ';
                batch += value;
            }
            batch += '\n';
        }

        const auto sent = Clock::now();
        const bool got = conn.send(batch) && conn.read_lines(n, [&](std::string_view line) {
            res.latencies_ns.push_back(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sent).count()));
            if (line.starts_with("ERR")) ++res.errors;
            else if (line == "NOT_FOUND") ++res.misses;
        });
        if (!got) {
            res.failure = "connection lost after " + std::to_string(done) + " requests";
            return;
        }
        done += n;
    }
}

double percentile_us(const std::vector<uint64_t>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t rank = static_cast<size_t>(std::ceil(p * static_cast<double>(sorted.size())));
    rank = std::clamp<size_t>(rank, 1, sorted.size());
    return static_cast<double>(sorted[rank - 1]) / 1000.0;
}

}

int main(int argc, char** argv) {
    const auto opts = parse_args(argc, argv);
    if (!opts) return 2;
    const Options& o = *opts;

    std::vector<ClientResult> results(o.clients);
    std::latch start_line(static_cast<std::ptrdiff_t>(o.clients) + 1);
    std::vector<std::thread> threads;
    threads.reserve(o.clients);
    for (size_t i = 0; i < o.clients; ++i) {
        threads.emplace_back(run_client, std::cref(o), i, std::ref(start_line), std::ref(results[i]));
    }

    start_line.arrive_and_wait();
    const auto t0 = Clock::now();
    for (auto& t : threads) t.join();
    const double seconds = std::chrono::duration<double>(Clock::now() - t0).count();

    std::vector<uint64_t> all;
    uint64_t errors = 0, misses = 0;
    bool failed = false;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].failure.empty()) {
            std::cerr << "client " << i << ": " << results[i].failure << '\n';
            failed = true;
        }
        all.insert(all.end(), results[i].latencies_ns.begin(), results[i].latencies_ns.end());
        errors += results[i].errors;
        misses += results[i].misses;
    }
    if (failed) return 1;
    std::sort(all.begin(), all.end());

    const int read_pct = static_cast<int>(std::lround(o.read_ratio * 100));
    const double ops = static_cast<double>(all.size()) / seconds;
    const double p50 = percentile_us(all, 0.50);
    const double p99 = percentile_us(all, 0.99);
    const double p999 = percentile_us(all, 0.999);
    const double max = all.empty() ? 0.0 : static_cast<double>(all.back()) / 1000.0;

    std::printf("loadgen: %zu clients x %zu requests, %d/%d read/write, %zu keys, %zu-byte values, pipeline %zu\n",
                o.clients, o.requests, read_pct, 100 - read_pct, o.keys, o.value_size, o.pipeline);
    std::printf("throughput  %.0f ops/sec  (%zu ops in %.2f s)\n", ops, all.size(), seconds);
    std::printf("latency     p50 %.1f us   p99 %.1f us   p99.9 %.1f us   max %.1f us\n", p50, p99, p999, max);
    std::printf("errors      %llu   GET misses %llu\n", static_cast<unsigned long long>(errors),
                static_cast<unsigned long long>(misses));
    std::printf("README row  | %zu | %d/%d | %zu | %.0f | %.1f | %.1f | %.1f |\n", o.clients, read_pct,
                100 - read_pct, o.pipeline, ops, p50, p99, p999);
    return errors > 0 ? 1 : 0;
}
