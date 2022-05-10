cmake_minimum_required(VERSION 3.9)
project(doryheartbeatbench C CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_definitions(-DSHM_MEM_KEY=0xf52482b8UL)

add_executable(zeroshm ${HEADER_TIDER} zero.c)
target_link_libraries(zeroshm ${CONAN_LIBS})

add_executable(computetime ${HEADER_TIDER} compute.c)
target_link_libraries(computetime ${CONAN_LIBS})

add_executable(generatedata ${HEADER_TIDER} generate.cpp)
target_link_libraries(generatedata ${CONAN_LIBS})
