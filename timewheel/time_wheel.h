#ifndef TIME_WHEEL_H
#define TIME_WHEEL_H

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <vector>
#include <algorithm>
#include <cmath>
#include <thread>
#include <list>
#include <iostream>
#include <optional>
#include <unordered_set>
#include <functional>

using namespace std::literals::chrono_literals;
using timer_id_t = std::size_t;
using time_point_t = std::chrono::high_resolution_clock::time_point;
using duration_t = std::chrono::microseconds;

struct closed_queue : std::exception {};

template <typename T, typename Clock = std::chrono::system_clock>
class delay_queue {
public:
    using value_type = T;
    using time_point = typename std::chrono::high_resolution_clock::time_point;

    delay_queue() = default;
    explicit delay_queue(std::size_t initial_capacity) { q_.reserve(initial_capacity); }
    ~delay_queue() = default;

    delay_queue(const delay_queue&) = delete;
    delay_queue& operator=(const delay_queue&) = delete;

    void enqueue(value_type v, time_point tp) {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        if (closed_)
            throw closed_queue{};
        q_.emplace_back(std::move(v), tp);
        std::stable_sort(begin(q_), end(q_), [](auto&& a, auto&& b) { return a.second > b.second; });
        cv_.notify_one();
    }

    value_type dequeue() {
        std::unique_lock<decltype(mtx_)> lk(mtx_);
        auto now = std::chrono::time_point<std::chrono::high_resolution_clock>(
            std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(Clock::now().time_since_epoch()));
        try {
            while (!(q_.empty() && closed_) && !(!q_.empty() && q_.back().second <= now)) {
                if (q_.empty())
                    cv_.wait(lk);
                else
                    cv_.wait_until(lk, q_.back().second);
                now = std::chrono::time_point<std::chrono::high_resolution_clock>(
                    std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(Clock::now().time_since_epoch()));
            }
            if (q_.empty() && closed_)
                return {};  // invalid value
            value_type ret = std::move(q_.back().first);
            q_.pop_back();
            if (q_.empty() && closed_)
                cv_.notify_all();
            return ret;
        } catch (const std::exception& e) {
            std::cerr << "Exception in dequeue: " << e.what() << std::endl;
            throw;
        }
    }

    void close() {
        std::lock_guard<decltype(mtx_)> lk(mtx_);
        closed_ = true;
        cv_.notify_all();
    }

private:
    std::vector<std::pair<value_type, time_point>> q_;
    bool closed_ = false;
    std::mutex mtx_;
    std::condition_variable cv_;
};

class TimerTask {
public:
    TimerTask(time_point_t exp, timer_id_t timer_id, std::function<void(void)> &&func) :
        expiration_(exp), timer_id_(timer_id), func_(std::move(func)) {}

    TimerTask(time_point_t exp, timer_id_t timer_id, duration_t duration, std::function<void(void)> &&func) :
        expiration_(exp), timer_id_(timer_id), duration_(duration), periodic_(true), func_(std::move(func)) {}

    [[nodiscard]] timer_id_t timerId() const { return timer_id_; }
    time_point_t expiration() { return expiration_; }
    [[nodiscard]] bool isPeriodic() const { return periodic_; }

    void updatePeriodicExpiration() {
        //expiration_ = std::chrono::high_resolution_clock::now() + duration_;
        expiration_ = std::chrono::time_point<std::chrono::high_resolution_clock>(std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(std::chrono::system_clock::now().time_since_epoch())) + duration_;
    }

    void runTask() {
        if (isPeriodic()) {
            auto func_copy = func_;
            func_copy();
        } else {
            func_();
        }
    }

private:
    time_point_t expiration_;
    timer_id_t timer_id_{ 0 };
    duration_t duration_{ 0us };
    bool periodic_{ false };
    std::function<void(void)> func_;
};

class Bucket {
public:
    using tasks_list = std::list<std::unique_ptr<TimerTask>>;

    bool addTimer(std::unique_ptr<TimerTask> task, const time_point_t& task_expiration) {
        std::lock_guard lk(*mtx_);
        tasks_.emplace_back(std::move(task));
        auto x = task_expiration.time_since_epoch().count();
        auto y = expiration_.time_since_epoch().count();

        if (x != y) {
            expiration_ = task_expiration;
            return true;
        } else {
            return false;
        }
    }

    tasks_list extractTimers() {
        std::lock_guard lk(*mtx_);
        tasks_list tl = std::move(tasks_);
        return tl;
    }
private:
    time_point_t expiration_;
    std::unique_ptr<std::mutex> mtx_{ std::make_unique<std::mutex>() };
    tasks_list tasks_;
};

class TimerWheel final {
public:
    using timer_queue = delay_queue<std::optional<std::tuple<std::size_t, std::size_t, time_point_t>>>;

    TimerWheel(duration_t min_tick, std::size_t wheel_size, std::size_t wheel_cnt) :
        wheel_size_(wheel_size),
        wheel_cnt_(wheel_cnt),
        ticks_(wheel_cnt_),
        intervals_(wheel_cnt_),
        current_times_(wheel_cnt_),
        wheels_(wheel_cnt_),
        q_(std::make_shared<timer_queue>(wheel_size_ * wheel_cnt_)) {

        // auto now = std::chrono::system_clock::now();

        auto now = std::chrono::time_point<std::chrono::high_resolution_clock>(
            std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(std::chrono::system_clock::now().time_since_epoch()));

        duration_t tick_us(min_tick);
        for (auto i = 0; i < current_times_.size(); i++) {
            auto tick = tick_us * static_cast<std::size_t>(std::pow(wheel_size_, i));
            auto interval = tick_us * static_cast<std::size_t>(std::pow(wheel_size_, i + 1));
            ticks_[i] = tick;
            intervals_[i] = interval;
            current_times_[i] = timeTruncate(now, tick);
        }

        poll_thread_ = std::thread([&]() {
            for (auto ele = q_->dequeue(); ele != std::nullopt; ele = q_->dequeue()) {
                auto wheel_index = std::get<0>(*ele);
                auto bucket_index = std::get<1>(*ele);
                auto expiration = std::get<time_point_t>(*ele);
                // 更新所有轮子的时钟
                for (auto i = 0; i < current_times_.size(); i++) {
                    const auto& tick = ticks_[i];
                    auto& current_time = current_times_[i];
                    if (expiration >= (current_time + tick)) {
                        current_time = timeTruncate(expiration, tick);
                    }
                }

                auto& wheel = wheels_[wheel_index];
                auto& bucket = wheel[bucket_index];
                auto tl = bucket.extractTimers();
                for (auto&& task : tl) {
                    if (isTimerDeactivated(task->timerId())) {
                        continue;
                    }
                    addTimer(std::move(task));
                }
            }
        });

        // 为每个层级的时间轮分配 bucket
        for (auto& buckets : wheels_) {
            buckets.reserve(wheel_size_);
            for (auto bucket_index = 0; bucket_index < wheel_size_; bucket_index++) {
                buckets.emplace_back();
            }
        }
    }

    ~TimerWheel() {
        stop();
    }

    void stop() {
        if (poll_thread_.joinable()) {
            q_->close();
            poll_thread_.join();
        }
    }

    [[nodiscard]] bool isTimerDeactivated(timer_id_t timer_id) {
        std::lock_guard lk(cancel_timer_mtx_);
        return timer_ids_.find(timer_id) == timer_ids_.end();
    }

    void deactivateTimer(timer_id_t timer_id) {
        std::lock_guard lk(cancel_timer_mtx_);
        timer_ids_.erase(timer_id);
    }

    std::optional<timer_id_t> execEveryAfter(duration_t duration, std::function<void(void)> func) {
        auto timer_id = getTimerId();
        preActivateTimerId(timer_id);
        try {
            if (addTimer(std::make_unique<TimerTask>(std::chrono::high_resolution_clock::now() + duration, timer_id, duration, std::move(func)))) {
                return timer_id;
            }
        } catch (const closed_queue& e) {}
        deactivateTimer(timer_id);
        return {};
    }

    std::optional<timer_id_t> execEverySince(time_point_t time_point, duration_t duration, std::function<void(void)> func) {
        auto timer_id = getTimerId();
        preActivateTimerId(timer_id);
        try {
            if (addTimer(std::make_unique<TimerTask>(time_point, timer_id, duration, std::move(func)))) {
                return timer_id;
            }
        } catch (const closed_queue& e) {}
        deactivateTimer(timer_id);
        return {};
    }

    std::optional<timer_id_t> execAfter(duration_t duration, std::function<void(void)> func) {
        auto timer_id = getTimerId();
        preActivateTimerId(timer_id);
        try {
            if (addTimer(std::make_unique<TimerTask>(std::chrono::high_resolution_clock::now() + duration, timer_id, std::move(func)))) {
                return timer_id;
            }
        } catch (const closed_queue& e) {}
        deactivateTimer(timer_id);
        return {};
    }

    std::optional<timer_id_t> execAt(time_point_t time_point, std::function<void(void)> func) {
        auto timer_id = getTimerId();
        preActivateTimerId(timer_id);
        try {
            if (addTimer(std::make_unique<TimerTask>(time_point, timer_id, std::move(func)))) {
                return timer_id;
            }
        } catch (const closed_queue& e) {}
        deactivateTimer(timer_id);
        return {};
    }

private:
    [[nodiscard]] static time_point_t timeTruncate(const time_point_t& start_us, const duration_t& tick_us) {
        if (tick_us <= 0us) {
            return start_us;
        }
        return start_us - start_us.time_since_epoch() % tick_us;
    }

    void preActivateTimerId(timer_id_t timer_id) {
        std::lock_guard lk(cancel_timer_mtx_);
        timer_ids_.insert(timer_id);
    }

    [[nodiscard]] timer_id_t getTimerId() {
        std::lock_guard lk(timer_generator_mtx_);
        ++timer_cnt_;
        return timer_cnt_ > 0 ? timer_cnt_ : ++timer_cnt_;
    }

    bool addTimer(std::unique_ptr<TimerTask> task) {
        while (true) {
            auto task_expiration = task->expiration();
            auto reinsert = false;

            for (auto i = 0; i < wheels_.size(); i++) {
                auto now = current_times_[i];
                auto tick = ticks_[i];

                if (task_expiration < (now + tick)) {
                    task->runTask();
                    if (task->isPeriodic()) {
                        task->updatePeriodicExpiration();
                        reinsert = true;
                        break;
                    } else {
                        deactivateTimer(task->timerId());
                        return true;
                    }
                } else if (auto interval = intervals_[i]; task_expiration < (now + interval)) {
                    auto tick_nanoseconds = tick.count() * 1000;
                    auto virtual_id = task_expiration.time_since_epoch().count() / tick_nanoseconds;
                    auto expiration = std::chrono::high_resolution_clock::time_point(std::chrono::nanoseconds(virtual_id * tick_nanoseconds));

                    auto& wheel = wheels_[i];
                    auto bucket_index = virtual_id % wheel_size_;
                    auto& bucket = wheel[bucket_index];

                    if (bucket.addTimer(std::move(task), expiration)) {
                        q_->enqueue(std::make_tuple(i, bucket_index, expiration), expiration);
                    }
                    return true;
                } else {
                    continue;
                }
            }

            if (!reinsert) {
                return false;
            }
        }
    }

    const std::size_t wheel_size_;
    const std::size_t wheel_cnt_;
    std::vector<duration_t> ticks_;
    std::vector<duration_t> intervals_;
    std::vector<time_point_t> current_times_;
    std::vector<std::vector<Bucket>> wheels_;
    std::shared_ptr<timer_queue> q_;
    std::thread poll_thread_;

    std::mutex timer_generator_mtx_;
    timer_id_t timer_cnt_{ 0 };
    std::mutex cancel_timer_mtx_;
    std::unordered_set<timer_id_t> timer_ids_;
};

#endif // TIME_WHEEL_H
