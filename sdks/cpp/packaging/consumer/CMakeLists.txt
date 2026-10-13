# A consumer of the installed SDK, built by packaging/release.sh to prove the
# CMake config package works from an install prefix and not only from this tree.
cmake_minimum_required(VERSION 3.20)
project(loams-consumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(loams 0.1 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE loams::loams)
