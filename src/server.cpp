#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aof.hpp"
#include "fd.hpp"
#include "protocol.hpp"
#include "store.hpp"
#include "thread_pool.hpp"

namespace {

std::atomic<bool> g_stop{false};

extern "C" void handle_stop_signal(int) { g_stop.store(true); }

constexpr size_t kReadChunk = 64 * 1024;
constexpr size_t kMaxPendingInput = 16 * 1024 * 1024;

struct Config {
    std::string bind = "127.0.0.1";
    uint16_t port = 6380;
    size_t threads = 64;
    size_t shards = 16;
    bool aof_enabled = true;
    std::string aof_path = "data.aof";
    kv::FsyncPolicy fsync = kv::FsyncPolicy::EverySec;
    bool batch = true;
};

void usage(const char* argv0) {
    std::cerr << "usage: " << argv0 << " [options]\n"
              << "  --bind ADDR            IPv4 address to listen on (default 127.0.0.1)\n"
              << "  --port N               TCP port (default 6380)\n"
              << "  --threads N            worker threads = max concurrent clients (default 64)\n"
              << "  --shards N             independently locked store shards (default 16; 1 = single lock)\n"
              << "  --aof PATH             append-only log file (default data.aof)\n"
              << "  --no-aof               keep data in memory only\n"
              << "  --fsync POLICY         always | everysec | no (default everysec)\n"
              << "  --batch on|off         one send per batch of commands, or one per command (default on)\n";
}

template <class T>
bool parse_number(std::string_view s, T& out) {
    auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && end == s.data() + s.size();
}

std::optional<Config> parse_args(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto value = [&]() -> std::optional<std::string_view> {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << arg << '\n';
                return std::nullopt;
            }
            return std::string_view(argv[++i]);
        };
        auto bad = [&](std::string_view v) {
            std::cerr << "invalid value for " << arg << ": " << v << '\n';
            return std::nullopt;
        };

        if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return std::nullopt;
        } else if (arg == "--no-aof") {
            cfg.aof_enabled = false;
        } else if (arg == "--bind") {
            auto v = value();
            if (!v) return std::nullopt;
            cfg.bind = std::string(*v);
        } else if (arg == "--port") {
            auto v = value();
            if (!v) return std::nullopt;
            if (!parse_number(*v, cfg.port) || cfg.port == 0) return bad(*v);
        } else if (arg == "--threads") {
            auto v = value();
            if (!v) return std::nullopt;
            if (!parse_number(*v, cfg.threads) || cfg.threads == 0) return bad(*v);
        } else if (arg == "--shards") {
            auto v = value();
            if (!v) return std::nullopt;
            if (!parse_number(*v, cfg.shards) || cfg.shards == 0) return bad(*v);
        } else if (arg == "--aof") {
            auto v = value();
            if (!v) return std::nullopt;
            cfg.aof_path = std::string(*v);
        } else if (arg == "--fsync") {
            auto v = value();
            if (!v) return std::nullopt;
            auto policy = kv::parse_fsync_policy(*v);
            if (!policy) return bad(*v);
            cfg.fsync = *policy;
        } else if (arg == "--batch") {
            auto v = value();
            if (!v) return std::nullopt;
            if (*v == "on") cfg.batch = true;
            else if (*v == "off") cfg.batch = false;
            else return bad(*v);
        } else {
            std::cerr << "unknown option: " << arg << '\n';
            usage(argv[0]);
            return std::nullopt;
        }
    }
    return cfg;
}

class ConnRegistry {
public:
    void add(int fd) {
        std::lock_guard lock(mu_);
        fds_.insert(fd);
    }
    void remove(int fd) {
        std::lock_guard lock(mu_);
        fds_.erase(fd);
    }
    void shutdown_all() {
        std::lock_guard lock(mu_);
        for (int fd : fds_) ::shutdown(fd, SHUT_RDWR);
    }

private:
    std::mutex mu_;
    std::unordered_set<int> fds_;
};

void serve_client(int raw_fd, kv::Store& store, kv::Aof* aof, ConnRegistry& registry, bool batch) {
    kv::Fd fd(raw_fd);
    struct Unregister {
        ConnRegistry& registry;
        int fd;
        ~Unregister() { registry.remove(fd); }
    } unregister{registry, raw_fd};

    std::vector<char> chunk(kReadChunk);
    std::string in;
    std::string out;
    uint64_t pending_seq = 0;

    auto flush = [&]() -> bool {
        if (out.empty()) return true;
        if (aof && pending_seq != 0) aof->wait_durable(pending_seq);
        pending_seq = 0;
        const bool ok = kv::send_all(fd.get(), out);
        out.clear();
        return ok;
    };

    try {
        while (true) {
            const ssize_t n = ::recv(fd.get(), chunk.data(), chunk.size(), 0);
            if (n == 0) return;
            if (n < 0) {
                if (errno == EINTR) continue;
                return;
            }

            size_t scan_from = in.size();
            in.append(chunk.data(), static_cast<size_t>(n));
            size_t start = 0;
            for (size_t nl; (nl = in.find('\n', scan_from)) != std::string::npos; start = scan_from = nl + 1) {
                const std::string_view line(in.data() + start, nl - start);
                uint64_t seq = 0;
                kv::execute_into(store, kv::parse(line), out, &seq);
                pending_seq = std::max(pending_seq, seq);
                if (!batch && !flush()) return;
            }
            in.erase(0, start);

            if (in.size() > kMaxPendingInput) {
                out += "ERR line too long\n";
                flush();
                return;
            }
            if (!flush()) return;
        }
    } catch (const std::exception& e) {
        std::cerr << "kvserver: dropping client: " << e.what() << '\n';
    }
}

kv::Fd make_listener(const Config& cfg) {
    kv::Fd fd(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    if (!fd.valid()) throw std::system_error(errno, std::generic_category(), "socket");

    int one = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) != 0) {
        throw std::system_error(errno, std::generic_category(), "setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg.port);
    if (::inet_pton(AF_INET, cfg.bind.c_str(), &addr.sin_addr) != 1) {
        throw std::invalid_argument("not an IPv4 address: " + cfg.bind);
    }
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "bind " + cfg.bind + ":" + std::to_string(cfg.port));
    }
    if (::listen(fd.get(), 1024) != 0) throw std::system_error(errno, std::generic_category(), "listen");
    return fd;
}

void install_signal_handlers() {
    std::signal(SIGPIPE, SIG_IGN);

    struct sigaction sa {};
    sa.sa_handler = handle_stop_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
}

}

int main(int argc, char** argv) {
    const auto cfg = parse_args(argc, argv);
    if (!cfg) return 2;
    install_signal_handlers();

    try {
        kv::Store store(cfg->shards);
        std::unique_ptr<kv::Aof> aof;

        if (cfg->aof_enabled) {
            const auto t0 = std::chrono::steady_clock::now();
            const kv::ReplayStats stats = kv::Aof::replay(
                cfg->aof_path, [&](kv::LogOp op, std::string_view key, std::string_view value) {
                    if (op == kv::LogOp::Set) store.set(key, value);
                    else store.del(key);
                });
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            std::cerr << "kvserver: replayed " << stats.records << " records (" << store.size()
                      << " keys) from " << cfg->aof_path << " in " << ms << " ms\n";
            if (stats.truncated_bytes > 0) {
                std::cerr << "kvserver: discarded " << stats.truncated_bytes
                          << " bytes of torn or corrupt log tail\n";
            }
            aof = std::make_unique<kv::Aof>(cfg->aof_path, cfg->fsync);
            store.attach_log(aof.get());
        }

        const kv::Fd listener = make_listener(*cfg);
        std::cerr << "kvserver: listening on " << cfg->bind << ':' << cfg->port
                  << " (threads=" << cfg->threads << ", shards=" << cfg->shards
                  << ", aof=" << (cfg->aof_enabled ? cfg->aof_path : std::string("off"))
                  << ", fsync=" << (cfg->aof_enabled ? kv::to_string(cfg->fsync) : "n/a")
                  << ", batch=" << (cfg->batch ? "on" : "off") << ")\n";

        ConnRegistry registry;
        {
            kv::ThreadPool pool(cfg->threads);
            kv::Aof* const log = aof.get();
            const bool batch = cfg->batch;

            while (!g_stop.load()) {
                pollfd pfd{listener.get(), POLLIN, 0};
                const int ready = ::poll(&pfd, 1, 200);
                if (ready < 0 && errno != EINTR) {
                    std::perror("kvserver: poll");
                    break;
                }
                if (ready <= 0) continue;

                const int client = ::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC);
                if (client < 0) {
                    if (errno == EMFILE || errno == ENFILE) {
                        std::perror("kvserver: accept");
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                    continue;
                }

                int one = 1;
                ::setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

                registry.add(client);
                pool.submit([client, &store, log, &registry, batch] {
                    serve_client(client, store, log, registry, batch);
                });
            }

            std::cerr << "kvserver: shutting down\n";
            registry.shutdown_all();
        }

        store.attach_log(nullptr);
    } catch (const std::exception& e) {
        std::cerr << "kvserver: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
