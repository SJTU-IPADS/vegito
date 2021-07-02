cmake_minimum_required(VERSION 3.2)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(vegito)

ADD_DEFINITIONS( -std=c++14 -Wno-register -Wno-placement-new)

set(CMAKE_CXX_COMPILER g++-8)

#find_library(jemalloc_lib NAMES jemalloc PATHS ./deps/jemalloc PATH_SUFFIXES lib
#               NO_DEFAULT_PATH)

             #if(NOT jemalloc_lib)

             #	set(jemalloc_lib "")

             #endif()

include_directories(./src)
include_directories(third_party/rdma_lib/include)

## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## without benchmark profiling 
# set (MACRO_FLAGS "-DBASE_LINE")
set (MACRO_FLAGS "")

## currently drtm in this codebase is not supported, i will fix this later 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g3 ${MACRO_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread  -O0 -g2 ${MACRO_FLAGS}")

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
file(GLOB RDMA_SOURCES "third_party/rdma_lib/src/*.cc" "third_party/micautil/*.cc")

## add main files

file(GLOB SOURCES
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                       # framework
          "src/memstore/*.cc"                                                                    # memstore
          "src/backup_store/*.cc"                                                                # backup_store
          # "src/db/*.cc" "src/db/txs/*.cc"                                                        # framework + CC
          "src/db/*.cc" "src/db/txs/dbtx*.cc" "src/db/txs/log_ts_manager.cc"                                                        # framework + CC
          "src/util/*.cc" "src/util/micautil/*.cc" "src/port/*.cc" "src/oltp/utils/*.cc"         # utils
          )

# ch
file(GLOB CH_SOURCES
          "src/app/ch/*.cc"                                                                      # htap app
          "src/app/ch/ch_query/*.cc"                                                           # htap app query part
          "src/app/ch/micro_query/*.cc"                                                        # htap app micro query part
          )

# micro index
file (GLOB MICRO_INDEX_SOURCES
           "src/app/micro_index/*.cc"
           )

# ch
add_executable(vegito 
               ${SOURCES} 
               ${CH_SOURCES} 
               ${MICRO_INDEX_SOURCES} 
               ${RDMA_SOURCES}
               )

#target_compile_options(vegito PRIVATE "-DOCC_TX" "-DEPOCH_BLOCK=1")
target_compile_options(vegito PRIVATE "-DOCC_TX")
target_link_libraries(vegito ibverbs ssmalloc boost_system boost_coroutine)

# for the ease of running
set(CMAKE_INSTALL_PREFIX ./)
install(TARGETS vegito   DESTINATION .)

# copy
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/run.sh" DESTINATION ${CMAKE_BINARY_DIR})
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run2.py" DESTINATION ${CMAKE_BINARY_DIR}/scripts)
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_util.py" DESTINATION ${CMAKE_BINARY_DIR}/scripts)
