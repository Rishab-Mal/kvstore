#pragma once

#include <cerrno>
#include <cstddef>
#include <string_view>

#include <sys/socket.h>
#include <unistd.h>

namespace kv {

class Fd {
public:
    Fd() = default;
    explicit Fd(int fd) noexcept : fd_(fd) {}
    ~Fd() { reset(); }

    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : fd_(other.release()) {}
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) reset(other.release());
        return *this;
    }

    int get() const noexcept { return fd_; }
    bool valid() const noexcept { return fd_ >= 0; }

    int release() noexcept {
        int fd = fd_;
        fd_ = -1;
        return fd;
    }

    void reset(int fd = -1) noexcept {
        if (fd_ >= 0) ::close(fd_);
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

inline bool write_all(int fd, std::string_view data) {
    while (!data.empty()) {
        ssize_t n = ::write(fd, data.data(), data.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data.remove_prefix(static_cast<size_t>(n));
    }
    return true;
}

inline bool send_all(int fd, std::string_view data) {
    while (!data.empty()) {
        ssize_t n = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data.remove_prefix(static_cast<size_t>(n));
    }
    return true;
}

}
