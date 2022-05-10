cmake_minimum_required(VERSION 3.9)
project(dorydemo CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(demo ${HEADER_TIDER} main.cpp)
target_link_libraries(demo ${CONAN_LIBS})

add_executable(simple_server ${HEADER_TIDER} simple/server.cpp)
target_link_libraries(simple_server ${CONAN_LIBS})

add_executable(simple_client ${HEADER_TIDER} simple/client.cpp)
target_link_libraries(simple_client ${CONAN_LIBS})

add_executable(herd_server_mukharon ${HEADER_TIDER} herd/mukharon/server.cpp)
target_link_libraries(herd_server_mukharon ${CONAN_LIBS})

add_executable(herd_client_mukharon ${HEADER_TIDER} herd/mukharon/client.cpp)
target_link_libraries(herd_client_mukharon ${CONAN_LIBS})

add_executable(herd_server_mu ${HEADER_TIDER} herd/mu/server.cpp)
target_link_libraries(herd_server_mu ${CONAN_LIBS})

add_executable(herd_client_mu ${HEADER_TIDER} herd/mu/client.cpp)
target_link_libraries(herd_client_mu ${CONAN_LIBS})

add_executable(sync_killer ${HEADER_TIDER} sync-killer.cpp)
target_link_libraries(sync_killer ${CONAN_LIBS})

add_executable(sync_measurer ${HEADER_TIDER} sync-measurer.cpp)
target_link_libraries(sync_measurer ${CONAN_LIBS})
