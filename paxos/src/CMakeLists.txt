cmake_minimum_required(VERSION 3.9)
project(dorypaxos CXX)

if(DORY_PAXOS_DM)
  if(DORY_CTRL_DM)
    add_definitions(-DDORY_PAXOS_DM)
  else()
    message(FATAL_ERROR "DeviceMemory has to be enabled in the ctrl package.")
  endif()
endif()

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_library(dorypaxos ${HEADER_TIDER} majority-reader.cpp)

if(DORY_INTERNAL_TESTING)
  enable_testing()
  include(GoogleTest)

  add_executable(decorator_test test/decorator-test.cpp)
  target_link_libraries(decorator_test ${CONAN_LIBS})
  gtest_discover_tests(decorator_test)
endif()
