cmake_minimum_required(VERSION 3.9)
project(doryud CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(doryud ${HEADER_TIDER} main.cpp)

target_link_libraries(doryud ${CONAN_LIBS})
