cmake_minimum_required(VERSION 3.2)
project(vegito)

set(CMAKE_C_COMPILER "gcc-8")
set(CMAKE_CXX_COMPILER "g++-8")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# TODO: only support single machine without RDMA
option (WITH_RDMA "use RDMA" ON)

option (WITH_EDGE_PROP "add edge property for certain edges" OFF)
option (WITH_GRAPE "use Grape query" OFF)
option (WITH_GAIA "use GAIA analysis" OFF)
option (WITH_V6D "use Vineyard for block allocation" OFF)
option (ENABLE_EPOCH "use Seg-CSR or Seg-CSR/TS" OFF)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

set(DEFAULT_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
        STRING "Choose the type of build." FORCE
    )
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "MinSizeRel" "RelWithDebInfo"
    )
else()
    message(STATUS "Setting build type to '${CMAKE_BUILD_TYPE}'.")
endif()

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no march native support.")
endif()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-register -Wno-placement-new -Wno-return-type -DBOOST_COROUTINES_NO_DEPRECATION_WARNING")

#### Option: RDMA
if(WITH_RDMA)
    add_definitions(-DWITH_RDMA)
endif(WITH_RDMA)

#### Option: Edge Property
if(WITH_EDGE_PROP)
    add_definitions(-DWITH_EDGE_PROP)
endif(WITH_EDGE_PROP)

#### Option: Grape
if(WITH_GRAPE)
    add_definitions(-DWITH_GRAPE)
endif(WITH_GRAPE)

#### Option: Epoch
if(ENABLE_EPOCH)
    add_definitions(-DENABLE_EPOCH)
endif(ENABLE_EPOCH)

#### Option: GAIA
if(WITH_GAIA)
    add_definitions(-DWITH_GAIA)
endif(WITH_GAIA)

#### Option: Vineyard
if(WITH_V6D)
    add_definitions(-DWITH_V6D)
    include_directories("../gae")   # FIXME: hard directory for shared_storage
endif(WITH_V6D)

#### Third-party dependency ####

find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

if(WITH_GRAPE)
    # find mpi---------------------------------------------------------------------
    find_package(MPI REQUIRED)
    message(STATUS "Found mpi    (include: ${MPI_CXX_INCLUDE_PATH}, library: ${MPI_CXX_LIBRARIES})")
    include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})
    link_libraries(${MPI_CXX_LIBRARIES})

    # find glog---------------------------------------------------------------------
    include("cmake/FindGlog.cmake")
    if (NOT GLOG_FOUND)
        message(FATAL_ERROR "glog not found, please install the glog library")
    else ()
        include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
        link_libraries(${GLOG_LIBRARIES})
    endif ()

    # find gflags-------------------------------------------------------------------
    include("cmake/FindGFlags.cmake")
    if (NOT GFLAGS_FOUND)
        message(STATUS "gflags not found, build without gflags")
    else ()
        include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
        link_libraries(${GFLAGS_LIBRARIES})
    endif ()
endif(WITH_GRAPE)

if(WITH_V6D)
    find_package(vineyard 0.6.0 REQUIRED)
    include_directories(${VINEYARD_INCLUDE_DIRS})
    link_libraries(${VINEYARD_LIBRARIES})
endif(WITH_V6D)

# find openmp-------------------------------------------------------------------
find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

# find TBB-------------------------------------------------------------------
find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

include_directories(./src)
include_directories(./src/livegraph)
include_directories(third_party)
include_directories(third_party/rdma_lib/include)

## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## currently drtm in this codebase is not supported, i will fix this later
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread  -O0 -g2")

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
file(GLOB RDMA_SOURCES "third_party/rdma_lib/src/*.cc")

## add main files

file(GLOB SOURCES
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                  # framework
          "src/memstore/*.cc"                                                               # memstore
          "src/backup_store/*.cc"                                                           # backup_store
          "src/db/*.cc" "src/db/txs/dbtx*.cc" "src/db/txs/log_ts_manager.cc"                # framework + CC
          "src/util/*.cc" "src/port/*.cc"    # utils
          )

# ch
file(GLOB CH_SOURCES
          "src/app/ch/*.cc"                    # htap app
          "src/app/ch/ch_query/*.cc"           # htap app query part
          "src/app/ch/micro_query/*.cc"        # htap app micro query part
          "src/app/ch/ch_analytics/*.cc"        # htap app micro query part
          )

# micro index
file (GLOB MICRO_INDEX_SOURCES
           "src/app/micro_index/*.cc"
           )

# graph
file (GLOB GRAPH_SOURCES
           "src/graph/*.cc"
           "src/livegraph/src/*.cpp"
           )

# grape
if(WITH_GRAPE)
    file(GLOB_RECURSE GRAPE_SOURCES
                "src/grape/*.cc"
                )
endif(WITH_GRAPE)

# Wiki
file (GLOB_RECURSE WIKI_SOURCES
           "src/app/wiki/*.cc"
           )

# LDBC
file (GLOB_RECURSE LDBC_SOURCES
           "src/app/ldbc/*.cc"
           "src/app/ldbc/ldbc_query/*.cc"
           )


file (GLOB SSMALLOC_SOURCES
           "third_party/rdma_lib/ralloc/*.c"
           )

add_executable(vegito
               ${SOURCES}
               ${RDMA_SOURCES}
               ${CH_SOURCES}
               ${MICRO_INDEX_SOURCES}
               ${GRAPH_SOURCES}
               ${WIKI_SOURCES}
               ${LDBC_SOURCES}
               ${GRAPE_SOURCES}
               ${SSMALLOC_SOURCES}
               )

target_compile_options(vegito PRIVATE "-DOCC_TX")
target_link_libraries(vegito ibverbs boost_system boost_coroutine)

include_directories(third_party/eigen)

# link against gaia_runtime
if(WITH_GAIA)
    set(GAIA_RUNTIME_LIBRARY_ROOT "${CMAKE_CURRENT_LIST_DIR}/src/interactive_engine/executor/target")

    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        if (EXISTS "${GAIA_RUNTIME_LIBRARY_ROOT}/release/libgaia_runtime.so")
            message(STATUS "Link against to the release build of gaia runtime")
            target_link_libraries(vegito ${GAIA_RUNTIME_LIBRARY_ROOT}/release/libgaia_runtime.so)
        else()
            message(STATUS "The release build of gaia runtime not found, fall back to the debug version")
        target_link_libraries(vegito ${GAIA_RUNTIME_LIBRARY_ROOT}/debug/libgaia_runtime.so)
        endif()
    else()
        message(STATUS "Link against to the debug build of gaia runtime")
        target_link_libraries(vegito ${GAIA_RUNTIME_LIBRARY_ROOT}/debug/libgaia_runtime.so)
    endif()
endif(WITH_GAIA)

# for the ease of running
set(CMAKE_INSTALL_PREFIX ./)
install(TARGETS vegito DESTINATION .)

# copy
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run.py" DESTINATION ${CMAKE_BINARY_DIR}/scripts)
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_util.py" DESTINATION ${CMAKE_BINARY_DIR}/scripts)
