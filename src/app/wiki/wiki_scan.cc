#include <chrono>
#include "wiki_scan.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {
WikiScanWorker::WikiScanWorker(std::string graph_type,
                                 std::shared_ptr<SegGraph> seg_graph,
                                 std::shared_ptr<Graph> live_graph,
                                 std::shared_ptr<CSRGraph> csr_graph) 
    : graph_type_(graph_type),
      seg_graph_(seg_graph),
      live_graph_(live_graph),
      csr_graph_(csr_graph) {
    if (graph_type_ == "seggraph") {
        num_vertices = seg_graph->get_max_vertex_id();
    } else if (graph_type_ == "livegraph") {
        num_vertices = live_graph->get_max_vertex_id();
    } else {
        num_vertices = csr_graph->get_max_vertex_id();
    }
}

void WikiScanWorker::run() {
    float min = 1000;
    while(true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        float ret = scan();
        min = ret < min ? ret : min;
        std::cout << "Minimum used time: " << min << " seconds" << std::endl;
    }
}

float WikiScanWorker::scan() {
    int cpu_num = BindToCore(10);
    // allocate context (including workload partition)
    nocc::util::Breakdown_Timer timer;
    auto seg_txn = seg_graph_->create_graph_reader(0);
    // auto seg_txn = seg_graph_->begin_read_only_transaction();
    auto lg_txn = live_graph_->begin_read_only_transaction();
    auto csr_txn = csr_graph_->begin_read_only_transaction();

    timer.start();
    if (graph_type_ == "seggraph") {
        segid_t seg_id = 0;
        while (true) {
            if (seg_id >= seg_graph_->get_max_seg_id()) break;
            auto segment = seg_txn.locate_segment(seg_id, 0);
            vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = std::min(num_vertices, seg_graph_->get_seg_end_vid(seg_id));
            for(vertex_t v_i = v_start; v_i < v_end; v_i++) {
                auto edge_iter = seg_txn.get_edges_in_seg(segment, v_i, false);
                while(edge_iter.valid()) {
                    volatile vertex_t dst = edge_iter.dst_id();
                    edge_iter.next();
                }
            }
            seg_id++;
        }
    } else if (graph_type_ == "livegraph") {
        for(vertex_t v_i = 0; v_i < num_vertices; v_i++) {
            auto edge_iter = lg_txn.get_edges(v_i, 0, true);
            while(edge_iter.valid()) {
                volatile vertex_t dst = edge_iter.dst_id();
                edge_iter.next();
            }
        }
    } else {
        for(vertex_t v_i = 0; v_i < num_vertices; v_i++) {
            auto edge_iter = csr_txn.get_edges(v_i, 0);
            while(edge_iter.valid()) {
                volatile vertex_t dst = edge_iter.dst_id();
                edge_iter.next();
            }
        }
    }

    float used_time = timer.get_diff_ms() / 1000;
    std::cout << "Finish scanning, "
              << " time:" << used_time << " seconds" << std::endl;
    return used_time;
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
