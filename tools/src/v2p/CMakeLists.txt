dory_setup_cmake()

add_executable(doryv2p ${HEADER_TIDER} v2p.cpp)

target_link_libraries(doryv2p ${CONAN_LIBS})
