cmake_minimum_required(VERSION 3.9)
project(crashconsensus CXX)

include(CheckIPOSupported)
check_ipo_supported(RESULT supported OUTPUT error)

if(supported)
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ${DORY_LTO})
else()
  message(STATUS "IPO / LTO not supported: <${error}>")
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CONAN_SYSTEM_INCLUDES On)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(NO_OUTPUT_DIRS)

message(STATUS "CMAKE_C_FLAGS: " ${CMAKE_C_FLAGS})
message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})
message(STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE})
message(STATUS "CMAKE_INTERPROCEDURAL_OPTIMIZATION: "
               ${CMAKE_INTERPROCEDURAL_OPTIMIZATION})

add_definitions(-DSPDLOG_ACTIVE_LEVEL=${SPDLOG_ACTIVE_LEVEL})

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)

add_library(membership-unique main.cpp)
target_link_libraries(membership-unique ${CONAN_LIBS})
set_target_properties(
  membership-unique
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib/unique"
             OUTPUT_NAME membership)

add_library(membership-nop main.cpp)
target_link_libraries(membership-nop ${CONAN_LIBS})
set_target_properties(
  membership-nop
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib/nop"
             OUTPUT_NAME membership)
target_compile_definitions(membership-nop PUBLIC -DMEMBERSHIP_NOP_LIB)
