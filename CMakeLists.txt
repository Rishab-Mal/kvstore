cmake_minimum_required(VERSION 3.20)
project(kvstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(KV_SANITIZER "" CACHE STRING "Sanitizer to build with: '', 'address', or 'thread'")
if(KV_SANITIZER STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZER STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(NOT KV_SANITIZER STREQUAL "")
  message(FATAL_ERROR "KV_SANITIZER must be empty, 'address', or 'thread'")
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

add_library(kvcore STATIC src/store.cpp src/protocol.cpp src/aof.cpp)
target_include_directories(kvcore PUBLIC src)
target_link_libraries(kvcore PUBLIC Threads::Threads)

add_executable(kvserver src/server.cpp)
target_link_libraries(kvserver PRIVATE kvcore)

add_executable(loadgen bench/loadgen.cpp)
target_include_directories(loadgen PRIVATE src)
target_link_libraries(loadgen PRIVATE Threads::Threads)

option(KV_BUILD_TESTS "Build unit tests (needs GoogleTest: apt install libgtest-dev)" ON)
if(KV_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_executable(kv_tests tests/test_store.cpp tests/test_protocol.cpp tests/test_aof.cpp)
  target_link_libraries(kv_tests PRIVATE kvcore GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(kv_tests)

  find_program(BASH bash)
  find_program(NC nc)
  if(BASH AND NC)
    add_test(NAME crash_recovery
             COMMAND ${BASH} ${CMAKE_CURRENT_SOURCE_DIR}/tests/crash_recovery.sh $<TARGET_FILE:kvserver>)
  endif()
endif()
