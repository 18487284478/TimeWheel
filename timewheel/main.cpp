#include <chrono>
#include <iostream>
#include <thread>

#include <string>
#include "time_wheel.h"

TimerWheel timer_wheel(10us, 30, 7);

typedef struct frame_t {
    uint64_t time;
    std::string text;
} frame;

std::chrono::high_resolution_clock::time_point posixToHighRes(uint64_t posixTime) {
    return std::chrono::high_resolution_clock::time_point(std::chrono::microseconds(posixTime));
}

void scheduled_print(uint64_t starttime , uint64_t posixTime, const std::string& message) {
    auto high_res_time_point = posixToHighRes(posixTime);
    timer_wheel.execAt(high_res_time_point, [starttime,posixTime,message]() {
        std::cout << "starttime:" << starttime << std::endl;
        std::cout << "posixTime:" << posixTime << std::endl;
        std::cout << message << std::endl;
    });
}

int main() {

    while (true) {
        auto posixNow = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
        uint64_t scheduledTime = posixNow + 1000000;
        scheduled_print(posixNow , scheduledTime, "Hello, World!");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return 0;
}
