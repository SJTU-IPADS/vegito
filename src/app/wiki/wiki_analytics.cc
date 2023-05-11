#include <chrono>
#include "wiki_analytics.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

#define DEBUG_LOG 0

WikiAnalyticsWorker::WikiAnalyticsWorker(uint32_t worker_id, uint32_t num_thread, std::string graph_type,
                                 std::shared_ptr<SegGraph> seg_graph,
                                 std::shared_ptr<Graph> live_graph,
                                 std::shared_ptr<CSRGraph> csr_graph,
                                 std::shared_ptr<Barrier> barrier) 
    : worker_id_(worker_id), 
      num_worker_(num_thread),
      is_master_worker_(worker_id_==0),
      graph_type_(graph_type),
      seg_graph_(seg_graph),
      live_graph_(live_graph),
      csr_graph_(csr_graph),
      complete_(false),
      barrier_(barrier),
      task_(nullptr) {
    
}

void WikiAnalyticsWorker::run() {
    /* Bind core */
    // int cpu_num = BindToCore(worker_id_);

    std::cout << "[Analytics Worker " << worker_id_ << "] " << std::endl;

    if(is_master_worker_) {
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            pagerank();
        }
    } else {
        // main loop for analytics worker
        while(true) {
            if (task_ == nullptr) {
                asm volatile("" ::: "memory");
                continue;
            }

            assert(!complete_);
            assert(ctx_ != nullptr);

            (this->*task_)();
            complete_ = true;

            task_ = nullptr;
            ctx_ = nullptr;
        }  // end main loop
    }
}

void WikiAnalyticsWorker::set_task(TaskFunc algo, std::shared_ptr<PageRankContext> ctx) {
    assert(task_ == nullptr);
    ctx_ = ctx;
    task_ = algo;
}

void WikiAnalyticsWorker::wait_for_complete() {
    while (!complete_) {
        asm volatile("" ::: "memory");
    }

    complete_ = false;
}

void WikiAnalyticsWorker::parallel_work(TaskFunc algo, std::shared_ptr<PageRankContext> ctx) {
    // partition task set
    if (graph_type_ == "seggraph") {
        for (int i = 0; i < sub_workers_.size(); i++) {
            segid_t seg_num = seg_graph_->get_max_seg_id()+1;

            sub_workers_[i]->set_start_seg(seg_num / sub_workers_.size() * i);
            if (i == sub_workers_.size() - 1) {
                sub_workers_[i]->set_end_seg(seg_graph_->get_max_seg_id());
            } else {
                sub_workers_[i]->set_end_seg(seg_num / sub_workers_.size() * (i+1));
            }
        }
    } else if (graph_type_ == "livegraph") {
        size_t basic_chunk = 4096;
        for (int i = 0; i < sub_workers_.size(); i++) {
            vertex_t vertex_num = live_graph_->get_max_vertex_id();

            sub_workers_[i]->set_start_vid(vertex_num / sub_workers_.size() / basic_chunk * basic_chunk * i);
            if (i == sub_workers_.size() - 1) {
                sub_workers_[i]->set_end_vid(live_graph_->get_max_vertex_id());
            } else {
                sub_workers_[i]->set_end_vid(vertex_num / sub_workers_.size() / basic_chunk * basic_chunk * (i+1));
            }
        }
    } else {
        size_t basic_chunk = 4096;
        for (int i = 0; i < sub_workers_.size(); i++) {
            vertex_t vertex_num = csr_graph_->get_max_vertex_id();
            sub_workers_[i]->set_start_vid(vertex_num / sub_workers_.size() / basic_chunk * basic_chunk * i);
            if (i == sub_workers_.size() - 1) {
                sub_workers_[i]->set_end_vid(csr_graph_->get_max_vertex_id());
            } else {
                sub_workers_[i]->set_end_vid(vertex_num / sub_workers_.size() / basic_chunk * basic_chunk * (i+1));
            }
        }
    }

    // start analytics work, skip itself
    for (int i = 1; i < sub_workers_.size(); ++i) {
        sub_workers_[i]->set_task(algo, ctx);
    }
  
    // do my work
    ctx_ = ctx;
    (this->*algo)();
  
    // wait for other threads, skip itself
    for (int i = 1; i < sub_workers_.size(); ++i) 
      sub_workers_[i]->wait_for_complete();
}

// process vertices
template<typename R>
R WikiAnalyticsWorker::process_vertices(std::function<R(vertex_t vtx)> vertex_func, Bitmap* active)
{
    auto& worker_states = ctx_->worker_states;
    auto& txn = ctx_->txn;
    R reducer = 0;

    worker_states[worker_id_].curr_seg = this->start_seg_;
    worker_states[worker_id_].end_seg = this->end_seg_;
    worker_states[worker_id_].curr_vid = this->start_vid_;
    worker_states[worker_id_].end_vid = this->end_vid_;
    worker_states[worker_id_].status = WorkerStatus::WORKING;

    size_t basic_chunk = 64;

    // handle work owned by self 
    while (true) {
        if (graph_type_ == "seggraph") {
            segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_id_].curr_seg, 1);
            if (seg_id >= worker_states[worker_id_].end_seg) break;
            vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
            for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                vertex_t local_v_i = v_i;
                unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                while (word != 0) {
                    if (word & 1) {
                        reducer += vertex_func(local_v_i);
                    }
                    local_v_i++;
                    word = word >> 1;
                }
            }
        } else {
            vertex_t v_i = __sync_fetch_and_add(&worker_states[worker_id_].curr_vid, basic_chunk);
            if (v_i >= worker_states[worker_id_].end_vid) break;
            // if(active->get_bit(v_i)) {
            //     reducer += vertex_func(v_i);
            // }
            unsigned long word = active->data[WORD_OFFSET(v_i)];
            while (word != 0) {
                if (word & 1) {
                    reducer += vertex_func(v_i);
                }
                v_i++;
                word = word >> 1;
            }
        }
    }

    // handle work owned by others 
    worker_states[worker_id_].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker_; t_offset++) {
        int t_i = (worker_id_ + t_offset) % num_worker_;
        while (worker_states[t_i].status != WorkerStatus::STEALING) {
            if (graph_type_ == "seggraph") {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg, 1);
                if (seg_id >= worker_states[t_i].end_seg) break;
                vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
                for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                    vertex_t local_v_i = v_i;
                    unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            reducer += vertex_func(local_v_i);
                        }
                        local_v_i++;
                        word = word >> 1;
                    }
                }
            } else {
                vertex_t v_i = __sync_fetch_and_add(&worker_states[t_i].curr_vid, basic_chunk);
                if (v_i >= worker_states[t_i].end_vid) break;
                unsigned long word = active->data[WORD_OFFSET(v_i)];
                while (word != 0) {
                    if (word & 1) {
                        reducer += vertex_func(v_i);
                    }
                    v_i++;
                    word = word >> 1;
                }
            }
        }
    }
    return reducer;
}

// process edges
template<typename R>
R WikiAnalyticsWorker::process_edges_push(label_t edge_label,
                                          std::function<R(vertex_t, EdgeIteratorBase&)> push_edge_func, 
                                          Bitmap * active) {
    auto& worker_states = ctx_->worker_states;
    auto& seg_txn = ctx_->txn;
    auto& lg_txn = ctx_->lg_txn;
    auto& csr_txn = ctx_->csr_txn;
    R reducer = 0;
    std::cout << "Push mode" << std::endl;

    worker_states[worker_id_].curr_seg = this->start_seg_;
    worker_states[worker_id_].end_seg = this->end_seg_;
    worker_states[worker_id_].curr_vid = this->start_vid_;
    worker_states[worker_id_].end_vid = this->end_vid_;
    worker_states[worker_id_].status = WorkerStatus::WORKING;

    if(DEBUG_LOG) {
        std::cout << "Analytics thread [" << this->worker_id_ << "] start:" 
                << this->start_vid_ << ", end:"
                << this->end_vid_ << std::endl;
    }

    size_t basic_chunk = 64;

    // handle work owned by self 
    while (true) {
        if (graph_type_ == "seggraph") {
            segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_id_].curr_seg, 1);
            if (seg_id >= worker_states[worker_id_].end_seg) break;
            auto segment = seg_txn.locate_segment(seg_id, edge_label);
            vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
            for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                // TODO: use outgoing_adj_bitmap to avoid empty computation
                vertex_t local_v_i = v_i;
                unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                while (word != 0) {
                    if (word & 1) {
                        auto edge_iter = seg_txn.get_edges_in_seg(segment, local_v_i, false);
                        reducer += push_edge_func(local_v_i, edge_iter);
                    }
                    local_v_i++;
                    word = word >> 1;
                }
            }
        } else if (graph_type_ == "livegraph") {
            vertex_t v_i = __sync_fetch_and_add(&worker_states[worker_id_].curr_vid, basic_chunk);
            if (v_i >= worker_states[worker_id_].end_vid) break;
            // if(active->get_bit(v_i)) {
            //     auto edge_iter = lg_txn.get_edges(v_i, edge_label, false);
            //     reducer += push_edge_func(v_i, edge_iter);
            // }
            unsigned long word = active->data[WORD_OFFSET(v_i)];
            while (word != 0) {
                if (word & 1) {
                    auto edge_iter = lg_txn.get_edges(v_i, edge_label, false);
                    reducer += push_edge_func(v_i, edge_iter);
                }
                v_i++;
                word = word >> 1;
            }
        } else {
            vertex_t v_i = __sync_fetch_and_add(&worker_states[worker_id_].curr_vid, basic_chunk);
            if (v_i >= worker_states[worker_id_].end_vid) break;
            unsigned long word = active->data[WORD_OFFSET(v_i)];
            while (word != 0) {
                if (word & 1) {
                    auto edge_iter = csr_txn.get_edges(v_i, edge_label);
                    reducer += push_edge_func(v_i, edge_iter);
                }
                v_i++;
                word = word >> 1;
            }
        }
    }

    // handle work owned by others 
    worker_states[worker_id_].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker_; t_offset++) {
        int t_i = (worker_id_ + t_offset) % num_worker_;
        if (worker_states[t_i].status == WorkerStatus::STEALING) continue;
        while (true) {
            if (graph_type_ == "seggraph") {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg, 1);
                if (seg_id >= worker_states[t_i].end_seg) break;
                auto segment = seg_txn.locate_segment(seg_id, edge_label);
                vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
                for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                    // TODO: use outgoing_adj_bitmap to avoid empty computation
                    vertex_t local_v_i = v_i;
                    unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            auto edge_iter = seg_txn.get_edges_in_seg(segment, local_v_i, false);
                            reducer += push_edge_func(local_v_i, edge_iter);
                        }
                        local_v_i++;
                        word = word >> 1;
                    }
                }
            } else if (graph_type_ == "livegraph") {
                vertex_t v_i = __sync_fetch_and_add(&worker_states[t_i].curr_vid, basic_chunk);
                if (v_i >= worker_states[t_i].end_vid) break;
                unsigned long word = active->data[WORD_OFFSET(v_i)];
                while (word != 0) {
                    if (word & 1) {
                        auto edge_iter = lg_txn.get_edges(v_i, edge_label, false);
                        reducer += push_edge_func(v_i, edge_iter);
                    }
                    v_i++;
                    word = word >> 1;
                }
            } else {
                vertex_t v_i = __sync_fetch_and_add(&worker_states[t_i].curr_vid, basic_chunk);
                if (v_i >= worker_states[t_i].end_vid) break;
                unsigned long word = active->data[WORD_OFFSET(v_i)];
                while (word != 0) {
                    if (word & 1) {
                        auto edge_iter = csr_txn.get_edges(v_i, edge_label);
                        reducer += push_edge_func(v_i, edge_iter);
                    }
                    v_i++;
                    word = word >> 1;
                }
            }
        }
    }

    return reducer;
}


template<typename R>
R WikiAnalyticsWorker::process_edges_pull(label_t edge_label,
                                          std::function<R(vertex_t, EdgeIteratorBase&)> pull_edge_func, 
                                          Bitmap * active) {
    auto& worker_states = ctx_->worker_states;
    auto& seg_txn = ctx_->txn;
    auto& lg_txn = ctx_->lg_txn;
    auto& csr_txn = ctx_->csr_txn;
    R reducer = 0;
    std::cout << "Pull mode" << std::endl;

    worker_states[worker_id_].curr_seg = this->start_seg_;
    worker_states[worker_id_].end_seg = this->end_seg_;
    worker_states[worker_id_].curr_vid = this->start_vid_;
    worker_states[worker_id_].end_vid = this->end_vid_;
    worker_states[worker_id_].status = WorkerStatus::WORKING;

    size_t basic_chunk = 4096;

    // handle work owned by self 
    while (true) {
        if (graph_type_ == "seggraph") {
            segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_id_].curr_seg, 1);
            if (seg_id >= worker_states[worker_id_].end_seg) break;
            auto segment = seg_txn.locate_segment(seg_id, edge_label);
            vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
            for (vertex_t v_i = v_start; v_i < v_end; v_i++) {
                // vertex_t v_i = compressed_incoming_adj_index[p_v_i].vertex;
                auto edge_iter = seg_txn.get_edges_in_seg(segment, v_i, true);
                reducer += pull_edge_func(v_i, edge_iter);
            }
        } else if (graph_type_ == "livegraph") {
            vertex_t v_start = __sync_fetch_and_add(&worker_states[worker_id_].curr_vid, basic_chunk);
            if (v_start >= worker_states[worker_id_].end_vid) break;
            for(vertex_t v_i = v_start; v_i < v_start + basic_chunk; v_i++) {
                auto edge_iter = lg_txn.get_edges(v_i, edge_label, true);
                reducer += pull_edge_func(v_i, edge_iter);
            }
        } else {
            vertex_t v_start = __sync_fetch_and_add(&worker_states[worker_id_].curr_vid, basic_chunk);
            if (v_start >= worker_states[worker_id_].end_vid) break;
            for(vertex_t v_i = v_start; v_i < v_start + basic_chunk; v_i++) {
                auto edge_iter = csr_txn.get_edges(v_i, edge_label);
                reducer += pull_edge_func(v_i, edge_iter);
            }
        }
    }

    // handle work owned by others 
    worker_states[worker_id_].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker_; t_offset++) {
        int t_i = (worker_id_ + t_offset) % num_worker_;
        while (worker_states[t_i].status != WorkerStatus::STEALING) {
            if(graph_type_ == "seggraph") {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg, 1);
                if (seg_id >= worker_states[t_i].end_seg) break;
                auto segment = seg_txn.locate_segment(seg_id, edge_label);
                vertex_t v_start = seg_graph_->get_seg_start_vid(seg_id), v_end = seg_graph_->get_seg_end_vid(seg_id);
                for (vertex_t v_i = v_start; v_i < v_end; v_i++) {
                    // vertex_t v_i = compressed_incoming_adj_index[p_v_i].vertex;
                    auto edge_iter = seg_txn.get_edges_in_seg(segment, v_i, true);
                    reducer += pull_edge_func(v_i, edge_iter);
                }
            } else if (graph_type_ == "livegraph") {
                vertex_t v_start = __sync_fetch_and_add(&worker_states[t_i].curr_vid, basic_chunk);
                if (v_start >= worker_states[t_i].end_vid) break;
                for(vertex_t v_i = v_start; v_i < v_start + basic_chunk; v_i++) {
                    auto edge_iter = lg_txn.get_edges(v_i, edge_label, true);
                    reducer += pull_edge_func(v_i, edge_iter);
                }
            } else {
                vertex_t v_start = __sync_fetch_and_add(&worker_states[t_i].curr_vid, basic_chunk);
                if (v_start >= worker_states[t_i].end_vid) break;
                for(vertex_t v_i = v_start; v_i < v_start + basic_chunk; v_i++) {
                    auto edge_iter = csr_txn.get_edges(v_i, edge_label);
                    reducer += pull_edge_func(v_i, edge_iter);
                }
            }
        }
    }

    return reducer;
}

void WikiAnalyticsWorker::pagerank_iter() {
    auto& param = ctx_->args;
    auto& txn = ctx_->txn;
    auto& worker_datas = ctx_->worker_datas;

    label_t label = param.label;
    int iterations = param.iterations;
    double* curr = param.curr;
    double* next = param.next;
    size_t* out_degree = param.out_degree;
    Bitmap * active = param.active;

    // 1. init rank value
    double delta = process_edges_push<double>(
        label,
        [&](vertex_t src, EdgeIteratorBase& outgoing_adj){
            curr[src] = 1;
            uint32_t deg = 0;
            while(outgoing_adj.valid()) {
                deg++;
                outgoing_adj.next();
            }
            if (deg > 0) {
                curr[src] /= deg;
            }
            out_degree[src] = deg;
            return 1;
        },
        active
    );
    worker_datas[worker_id_].delta = delta;
    barrier_->wait();

    // 2. execute pagerank iteratively
    for (int iter = 0; iter < iterations; iter++) {
        // master worker gather delta
        if (is_master_worker_) {
            double total_delta = 0;
            for(int i = 0; i < sub_workers_.size(); i++) {
                total_delta += worker_datas[i].delta;
            }
            std::cout << "delta(" << iter << ")=" << total_delta << std::endl;
        }

        // calculate active edges
        // worker_datas[worker_id_].active_edges = process_vertices<size_t>(
        //     [&](vertex_t vtx){
        //         return out_degree[vtx];
        //     },
        //     active
        // );

        barrier_->wait();

        // master worker gather active_edges
        if (is_master_worker_) {
            size_t total_active_edges = 0;
            for(int i = 0; i < sub_workers_.size(); i++) {
                total_active_edges += worker_datas[i].active_edges;
            }
        }

        bool push_mode = false;
        active->fill();

        // pagerank aggregate function
        if(push_mode) {
            process_edges_push<double>(label,
                [&](vertex_t src, EdgeIteratorBase& outgoing_adj){
                    while(outgoing_adj.valid()) {
                        vertex_t dst = outgoing_adj.dst_id();
                        //std::cout << "src: " << src << ", dst: " << dst << std::endl;
                        write_add(&next[dst], curr[src]);
                        outgoing_adj.next();
                    }
                    return 0;
                },
                active
            );
        } else {
            process_edges_pull<double>(label,
                [&](vertex_t dst, EdgeIteratorBase& incoming_adj) {
                    double sum = 0;
                    while(incoming_adj.valid()) {
                        vertex_t src = incoming_adj.dst_id();
                        sum += curr[src];
                        incoming_adj.next();
                    }
                    next[dst] = sum;
                    return 0;
                },
                active
            );
        }

        barrier_->wait();

        // pagerank apply function
        if (iter == iterations-1) {
            delta = process_vertices<double>(
                [&](vertex_t vtx) {
                    next[vtx] = 0.15 + 0.85 * next[vtx];
                    //std::cout << "iter:" << iter << ", pr:" << next[vtx] << std::endl;
                    return 0;
                },
                active
            );
        } else {
            delta = process_vertices<double>(
                [&](vertex_t vtx) {
                    next[vtx] = 0.15 + 0.85 * next[vtx];
                    //std::cout << "iter:" << iter << ", pr:" << next[vtx] << std::endl;
                    if (out_degree[vtx] > 0) {
                        next[vtx] /= out_degree[vtx];
                        return std::fabs(next[vtx] - curr[vtx]) * out_degree[vtx];
                    }
                    return std::fabs(next[vtx] - curr[vtx]);
                },
                active
            );
        }
        worker_datas[worker_id_].delta = delta;

        std::swap(curr, next);
        barrier_->wait();
    }

    // calculate pagerank value sum
    barrier_->wait();

    worker_datas[worker_id_].pr_sum = process_vertices<double>(
        [&](vertex_t vtx) {
            return curr[vtx];
        },
        active
    );

    barrier_->wait();

    // master worker gather pr_sum
    if (is_master_worker_) {
        double total_pr_sum = 0;
        for(int i = 0; i < sub_workers_.size(); i++) {
            total_pr_sum += worker_datas[i].pr_sum;
        }
        std::cout << "pr_sum = " << total_pr_sum << std::endl;
    }
}

void WikiAnalyticsWorker::pagerank() {
    // allocate context (including workload partition)
    nocc::util::Breakdown_Timer timer;
    timer.start();
    auto ctx = std::make_shared<PageRankContext>(seg_graph_, live_graph_, csr_graph_, num_worker_);
    if (graph_type_ == "seggraph") {
        ctx->args.iterations = 5;
        ctx->args.label = 0;
        ctx->args.curr = seg_graph_->alloc_vertex_array<double>(0);
        ctx->args.next = seg_graph_->alloc_vertex_array<double>(0);
        ctx->args.active = new Bitmap(seg_graph_->get_max_vertex_id());
        ctx->args.active->fill();
        ctx->args.out_degree = seg_graph_->alloc_vertex_array<size_t>(0);
    } else if (graph_type_ == "livegraph") {
        ctx->args.iterations = 5;
        ctx->args.label = 0;
        ctx->args.curr = live_graph_->alloc_vertex_array<double>(0);
        ctx->args.next = live_graph_->alloc_vertex_array<double>(0);
        ctx->args.active = new Bitmap(live_graph_->get_max_vertex_id());
        ctx->args.active->fill();
        ctx->args.out_degree = live_graph_->alloc_vertex_array<size_t>(0);
    } else {
        ctx->args.iterations = 5;
        ctx->args.label = 0;
        ctx->args.curr = csr_graph_->alloc_vertex_array<double>(0);
        ctx->args.next = csr_graph_->alloc_vertex_array<double>(0);
        ctx->args.active = new Bitmap(csr_graph_->get_max_vertex_id());
        ctx->args.active->fill();
        ctx->args.out_degree = csr_graph_->alloc_vertex_array<size_t>(0);
    }

    // work
    parallel_work(&WikiAnalyticsWorker::pagerank_iter, ctx);
    
    std::cout << "Finish pagerank, "
              << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
