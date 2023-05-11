#pragma once

#include <memory>

#include "core/segment_graph.hpp"
#include "core/segment_transaction.hpp"
#include "utils/thread.h"
#include "utils/timer.h"

namespace nocc {
namespace oltp {
namespace wiki {

class WikiUpdateWorker : public ndb_thread {
public:
    WikiUpdateWorker(uint32_t worker_id, uint32_t num_thread, 
                     std::shared_ptr<livegraph::SegGraph> graph, std::string dataset_dir);

    int get_worker_id() const { return worker_id_; }

    void wiki_update_seg_graph();

    void update();
    void run();

protected:
    int worker_id_;
    int num_thread_;
    
    std::shared_ptr<livegraph::SegGraph> graph_;
    std::string dataset_dir_;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
