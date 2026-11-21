#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

namespace kv {

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads) {
        if (num_threads == 0) num_threads = 1;
        workers_.reserve(num_threads);
        try {
            for (size_t i = 0; i < num_threads; ++i) workers_.emplace_back([this] { run(); });
        } catch (...) {
            shutdown();
            throw;
        }
    }

    ~ThreadPool() { shutdown(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void submit(std::function<void()> task) {
        {
            std::lock_guard lock(mu_);
            if (stop_) throw std::runtime_error("submit on a stopped ThreadPool");
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

    size_t size() const noexcept { return workers_.size(); }

private:
    void shutdown() {
        {
            std::lock_guard lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    void run() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(mu_);
                cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                if (tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "thread pool: task threw: " << e.what() << '\n';
            } catch (...) {
                std::cerr << "thread pool: task threw a non-std exception\n";
            }
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> tasks_;
    bool stop_ = false;
    std::vector<std::thread> workers_;
};

}
