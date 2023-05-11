#pragma once

#include <memory>

#include "core/edge_iterator.hpp"
#include "core/graph.hpp"
#include "core/transaction.hpp"
#include "core/segment_graph.hpp"
#include "core/segment_transaction.hpp"
#include "core/csr_graph.hpp"
#include "core/csr_transaction.hpp"
#include "core/epoch_graph_reader.hpp"
#include "core/epoch_graph_writer.hpp"

#include "utils/thread.h"
#include "utils/barrier.h"
#include "utils/timer.h"

namespace nocc {
namespace oltp {
namespace wiki {

// XXX: expoit namespace in the header file
using namespace livegraph;

class WikiScanWorker : public ndb_thread {
public:
    WikiScanWorker(std::string graph_type, 
                        std::shared_ptr<SegGraph> seg_graph,
                        std::shared_ptr<Graph> live_graph,
                        std::shared_ptr<CSRGraph> csr_graph);

    void run();
    float scan();

protected:
    std::string graph_type_;
    std::shared_ptr<SegGraph> seg_graph_;
    std::shared_ptr<Graph> live_graph_;
    std::shared_ptr<CSRGraph> csr_graph_;
    vertex_t num_vertices;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
