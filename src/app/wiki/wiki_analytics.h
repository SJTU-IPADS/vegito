#pragma once

#include <memory>

#include "core/edge_iterator.hpp"
#include "core/graph.hpp"
#include "core/transaction.hpp"
#include "core/segment_graph.hpp"
#include "core/segment_transaction.hpp"
#include "core/csr_graph.hpp"
#include "core/csr_transaction.hpp"

#include "utils/thread.h"
#include "utils/barrier.h"
#include "utils/timer.h"

namespace nocc {
namespace oltp {
namespace wiki {

// XXX: expoit namespace in the header file
using namespace livegraph;

class WikiAnalyticsWorker : public ndb_thread {

    using TaskFunc = void (WikiAnalyticsWorker::*)();

    enum class WorkerStatus {
        WORKING,
        STEALING
    };

    struct WorkerState {
        segid_t curr_seg;
        segid_t end_seg;
        vertex_t curr_vid;
        vertex_t end_vid;
        WorkerStatus status;
    };

    struct PageRankParam {
        int curr_iter;
        int iterations;
        label_t label;
        double* curr;
        double* next;
        Bitmap* active;
        size_t* out_degree;
    };

    struct PageRankData {
        double delta;
        size_t active_edges;
        double pr_sum;
    };

    class PageRankContext {
    public:
        PageRankContext(std::shared_ptr<SegGraph> seg_graph,
                        std::shared_ptr<Graph> live_graph,
                        std::shared_ptr<CSRGraph> csr_graph,
                        int num_workers)
         : txn(seg_graph->begin_read_only_transaction()), 
           lg_txn(live_graph->begin_read_only_transaction()),
           csr_txn(csr_graph->begin_read_only_transaction()),
           worker_states(num_workers),
           worker_datas(num_workers) {}

        PageRankParam args;
        SegTransaction txn;
        Transaction lg_txn;
        CSRTransaction csr_txn;
        std::vector<WorkerState> worker_states;
        std::vector<PageRankData> worker_datas;
    };

public:
    WikiAnalyticsWorker(uint32_t worker_id, uint32_t num_thread, std::string graph_type, 
                        std::shared_ptr<SegGraph> seg_graph,
                        std::shared_ptr<Graph> live_graph,
                        std::shared_ptr<CSRGraph> csr_graph,
                        std::shared_ptr<Barrier> barrier);

    void run();

    inline int get_worker_id() const { return worker_id_; }

    inline void set_start_seg(segid_t seg) { this->start_seg_ = seg; }

    inline void set_end_seg(segid_t seg) { this->end_seg_ = seg; }

    inline void set_start_vid(vertex_t vid) { this->start_vid_ = vid; }

    inline void set_end_vid(vertex_t vid) { this->end_vid_ = vid; }

    void set_task(TaskFunc task, std::shared_ptr<PageRankContext> ctx);

    inline void set_sub_workers(std::vector<std::shared_ptr<WikiAnalyticsWorker>> sub_workers) {
        this->sub_workers_ = sub_workers;
    }
    
    void wait_for_complete();

    void parallel_work(TaskFunc algo, std::shared_ptr<PageRankContext> ctx);

    // process vertices
    template<typename R>
    R process_vertices(std::function<R(vertex_t)> func,
                       Bitmap * active);

    // process edges
    template<typename R>
    R process_edges_push(label_t edge_label,
                         std::function<R(vertex_t, EdgeIteratorBase&)> push_func, 
                         Bitmap * active);
    template<typename R>
    R process_edges_pull(label_t edge_label,
                         std::function<R(vertex_t, EdgeIteratorBase&)> pull_func, 
                         Bitmap * active);

    void pagerank();
    void pagerank_iter();

protected:
    int worker_id_;
    int num_worker_;
    bool is_master_worker_;
    std::string graph_type_;

    std::shared_ptr<Barrier> barrier_;

    volatile bool complete_;
    TaskFunc volatile task_;
    std::shared_ptr<PageRankContext> ctx_;

    // only for master worker 
    std::vector<std::shared_ptr<WikiAnalyticsWorker>> sub_workers_;

    vertex_t start_vid_;
    vertex_t end_vid_;

    segid_t start_seg_;
    segid_t end_seg_;
    
    std::shared_ptr<SegGraph> seg_graph_;
    std::shared_ptr<Graph> live_graph_;
    std::shared_ptr<CSRGraph> csr_graph_;

    std::string algo_name_;

    nocc::util::Breakdown_Timer process_edges_timer;
    nocc::util::Breakdown_Timer get_edges_timer;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
