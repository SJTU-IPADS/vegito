cmake_minimum_required(VERSION 3.1)
project(gloabl_store_api C CXX)

if(POLICY CMP0025)
  cmake_policy(SET CMP0025 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
option(NDEBUG "use debug" ON)

SET(DEFAULT_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
        STRING "Choose the type of build." FORCE
    )
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "MinSizeRel" "RelWithDebInfo"
    )
endif()

option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

# reference: https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#always-full-rpath
include(GNUInstallDirs)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib:${CMAKE_INSTALL_PREFIX}/lib64")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wno-deprecated-declarations")

# find openmp-------------------------------------------------------------------
find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

#  we need edge src/dst ids in etable.
add_definitions(-DENDPOINT_LISTS)

add_library(native_store global_store_ffi.cc
                         graph_builder_ffi.cc
)


set(HTAP_ROOT_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../../../")

include_directories(${HTAP_ROOT_DIR}/src)
include_directories(${HTAP_ROOT_DIR}/src/livegraph)
include_directories(${HTAP_ROOT_DIR}/third_party)
#include_directories(${HTAP_ROOT_DIR}/third_party/eigen)
include_directories(${HTAP_ROOT_DIR}/third_party/rdma_lib/include)

include_directories(${HTAP_ROOT_DIR}/src/framework)
