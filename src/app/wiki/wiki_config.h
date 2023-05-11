#pragma once

#include <memory>
#include <cassert>
#include <vector>
#include <string>
#include "core/segment_graph.hpp"

namespace nocc {
namespace oltp {
namespace wiki {

class WikiConfig {
public:
    WikiConfig();
    void parse_args(int argc, char **argv);
    void parse_wiki_xml(const std::string &xml);

    void print() const;

    // threads
    inline int getNumQueryThreads() const { return query_nthreads_; }
    inline int getNumAnalyticsThreads() const { return analytics_nthreads_; }
    inline int getNumUpdateThreads() const { return update_nthreads_; }
    inline int getNumScanThreads() const { return scan_nthreads_; }

    // graph
    inline std::string getGraphType() const { return graph_type_; }

    // loading
    inline std::string getDatasetDir() const { return dataset_dir_; }
    inline int getFileNum() const { return file_num_; }

    // time
    inline double getEpochSec() const { return epoch_sec_; }
    inline int getRunSec() const { return run_sec_; }

    // bulk load or random insertion
    inline bool getRandom() const { return random_; }

protected:
    /* Runtime worker */
    int query_nthreads_ = 0;
    int analytics_nthreads_ = 0;
    int update_nthreads_ = 0;
    int scan_nthreads_ = 1;

    /* Graph store */
    std::string graph_type_ = "seggraph"; // csr / seggraph / livegraph
    bool random_ = false; // false: bulk load; true: random insertion

    /* Loading */
    std::string dataset_dir_ = "/mnt/nfs/graph_htap_data/wiki/id_wiki_";
    int file_num_ = 13;
    
    /* Time */
    double epoch_sec_ = 1;  // epoch time
    int run_sec_ = 200;
};

extern WikiConfig wikiConfig;

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
