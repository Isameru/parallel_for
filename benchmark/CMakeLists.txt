
project(parallel_for-benchmark)

find_package(
	Threads REQUIRED)

include_directories(
	../include)

add_executable(parallel_for-benchmark
	benchmark.cpp)

target_link_libraries(parallel_for-benchmark
	Threads::Threads)

set_property(TARGET parallel_for-benchmark PROPERTY CXX_STANDARD 11)
