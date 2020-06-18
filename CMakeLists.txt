cmake_minimum_required(VERSION 3.10)
project(LiveGraph VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no march native support.")
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
        "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif(NOT CMAKE_BUILD_TYPE)

include_directories(${PROJECT_SOURCE_DIR})

add_library(corelib STATIC
    src/graph.cpp
    src/transaction.cpp)
set_property(TARGET corelib PROPERTY POSITION_INDEPENDENT_CODE ON)

add_library(livegraph SHARED bind/livegraph.cpp src/graph.cpp src/transaction.cpp)
target_link_libraries(livegraph corelib)
install(TARGETS livegraph DESTINATION lib)
install(FILES bind/livegraph.hpp DESTINATION include)

set(PROJECT_DEPS_DIR ${PROJECT_SOURCE_DIR}/deps)

option(BUILD_TESTING "Build the testing tree." ON)
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
    add_subdirectory(${PROJECT_DEPS_DIR}/doctest EXCLUDE_FROM_ALL)
    add_executable(tests
        test/main.cpp
        test/allocator.cpp
        test/blocks.cpp
        test/block_manager.cpp
        test/bloom_filter.cpp
        test/futex.cpp
        test/graph.cpp
        test/transaction.cpp
        test/utils.cpp
        bind/livegraph.cpp)
    target_link_libraries(tests corelib doctest::doctest)
    enable_testing()
    include(${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake)
    doctest_discover_tests(tests)
endif()
