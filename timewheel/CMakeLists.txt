cmake_minimum_required(VERSION 3.22)
project(timewheel)

if(MSVC)
    add_compile_options(/utf-8)
endif ()

include_directories(${COMMON_INCLUDE})

set(CMAKE_CXX_STANDARD 17)

add_executable(timewheel main.cpp time_wheel.h)

if (WIN32)
target_link_libraries(timewheel ws2_32)
target_compile_definitions(timewheel PRIVATE _WIN32_WINNT=0x0A00)
endif ()

