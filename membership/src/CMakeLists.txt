cmake_minimum_required(VERSION 3.9)
project(dorymembership CXX)

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

if("$ENV{CLANG_GEN_TIDY_INFO}" STREQUAL "1")
  add_library(dorymembership ${HEADER_TIDER})
endif()

add_executable(membership_member ${HEADER_TIDER} member.cpp)
target_link_libraries(membership_member ${CONAN_LIBS})

add_executable(membership_acceptor ${HEADER_TIDER} acceptor.cpp)
target_link_libraries(membership_acceptor ${CONAN_LIBS})

add_executable(membership_cache ${HEADER_TIDER} cache.cpp)
target_link_libraries(membership_cache ${CONAN_LIBS})

#
# Tests #
#

add_executable(test_clk_sync ${HEADER_TIDER} benchmark/test-clk-sync.cpp)
target_link_libraries(test_clk_sync ${CONAN_LIBS})

add_executable(test_prctl_proxy ${HEADER_TIDER} benchmark/prctl-proxy.cpp)
target_link_libraries(test_prctl_proxy ${CONAN_LIBS})

add_executable(test_clk_overhead ${HEADER_TIDER} benchmark/test-active.cpp)
target_compile_definitions(test_clk_overhead PUBLIC -DNO_POLLING
                                                    -DTEST_STEADY_CLK_OVERHEAD)
target_link_libraries(test_clk_overhead ${CONAN_LIBS})

# ##############################################################################
# Tests on member accessing majority of acceptors #
# ##############################################################################

add_executable(test_maj_isactive_overhead ${HEADER_TIDER}
                                          benchmark/test-active.cpp)
target_compile_definitions(test_maj_isactive_overhead
                           PUBLIC -DMAJORITY_POLLING -DTEST_IS_ACTIVE_OVERHEAD)
target_link_libraries(test_maj_isactive_overhead ${CONAN_LIBS})

add_executable(test_maj_view_change_time ${HEADER_TIDER}
                                         benchmark/test-active.cpp)
target_compile_definitions(
  test_maj_view_change_time PUBLIC -DMAJORITY_POLLING
                                   -DTEST_MEMBERSHIP_CHANGE_TIME)
target_link_libraries(test_maj_view_change_time ${CONAN_LIBS})

add_executable(test_maj_inactivity_stable_view ${HEADER_TIDER}
                                               benchmark/test-active.cpp)
target_compile_definitions(
  test_maj_inactivity_stable_view PUBLIC -DMAJORITY_POLLING
                                         -DTEST_INACTIVITY_IN_STABLE_VIEW)
target_link_libraries(test_maj_inactivity_stable_view ${CONAN_LIBS})

add_executable(test_maj_crash_time ${HEADER_TIDER} benchmark/test-active.cpp)
target_compile_definitions(
  test_maj_crash_time PUBLIC -DMAJORITY_POLLING -DTEST_CRASH_TIME_TO_NEW_VIEW)
target_link_libraries(test_maj_crash_time ${CONAN_LIBS})

add_executable(test_maj_ask_remove ${HEADER_TIDER} benchmark/test-active.cpp)
target_compile_definitions(test_maj_ask_remove PUBLIC -DMAJORITY_POLLING
                                                      -DTEST_ASK_REMOVE)
target_link_libraries(test_maj_ask_remove ${CONAN_LIBS})

add_executable(test_maj_coordinated_failures ${HEADER_TIDER}
                                             benchmark/test-active.cpp)
target_compile_definitions(
  test_maj_coordinated_failures PUBLIC -DMAJORITY_POLLING
                                       -DTEST_COORDINATED_FAILURES)
target_link_libraries(test_maj_coordinated_failures ${CONAN_LIBS})

# ##############################################################################
# Tests on member accesses caches which acceses majority of acceptors #
# ##############################################################################

add_executable(test_cache_isactive_overhead ${HEADER_TIDER}
                                            benchmark/test-active.cpp)
target_compile_definitions(test_cache_isactive_overhead
                           PUBLIC -DCACHE_POLLING -DTEST_IS_ACTIVE_OVERHEAD)
target_link_libraries(test_cache_isactive_overhead ${CONAN_LIBS})

add_executable(test_cache_view_change_time ${HEADER_TIDER}
                                           benchmark/test-active.cpp)
target_compile_definitions(test_cache_view_change_time
                           PUBLIC -DCACHE_POLLING -DTEST_MEMBERSHIP_CHANGE_TIME)
target_link_libraries(test_cache_view_change_time ${CONAN_LIBS})

add_executable(test_cache_inactivity_stable_view ${HEADER_TIDER}
                                                 benchmark/test-active.cpp)
target_compile_definitions(
  test_cache_inactivity_stable_view PUBLIC -DCACHE_POLLING
                                           -DTEST_INACTIVITY_IN_STABLE_VIEW)
target_link_libraries(test_cache_inactivity_stable_view ${CONAN_LIBS})

add_executable(test_cache_crash_time ${HEADER_TIDER} benchmark/test-active.cpp)
target_compile_definitions(test_cache_crash_time
                           PUBLIC -DCACHE_POLLING -DTEST_CRASH_TIME_TO_NEW_VIEW)
target_link_libraries(test_cache_crash_time ${CONAN_LIBS})

add_executable(test_cache_ask_remove ${HEADER_TIDER} benchmark/test-active.cpp)
target_compile_definitions(test_cache_ask_remove PUBLIC -DCACHE_POLLING
                                                        -DTEST_ASK_REMOVE)
target_link_libraries(test_cache_ask_remove ${CONAN_LIBS})

add_executable(test_cache_coordinated_failures ${HEADER_TIDER}
                                               benchmark/test-active.cpp)
target_compile_definitions(test_cache_coordinated_failures
                           PUBLIC -DCACHE_POLLING -DTEST_COORDINATED_FAILURES)
target_link_libraries(test_cache_coordinated_failures ${CONAN_LIBS})
