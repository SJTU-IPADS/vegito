/*
 * The code is a part of our project called VEGITO, which retrofits
 * high availability mechanism to tame hybrid transaction/analytical
 * processing.
 *
 * Copyright (c) 2021 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/vegito
 *
 */

#ifndef ANALYTICS_WORKER_H
#define ANALYTICS_WORKER_H

#include "all.h"
#include "graph/ddl.h"

#include "framework.h"

#ifdef WITH_GRAPE
#include "app/analytics/grape/grape_app.h"
#endif

#include "core/livegraph.hpp"

#include "util/util.h"
#include "util/fast_random.h"
#include "util/barrier.h"

#include "backup_worker.h"

using namespace rdmaio;
using namespace nocc::db;
using namespace livegraph;
using namespace nocc::graph;

#define DEBUG_LOG 0

namespace {

// critical code for cpu binding
inline int assign_thread(int worker_id) {
  // return getCpuQry() - worker_id % 10;
  return worker_id;
}

}

namespace nocc {
namespace oltp {

struct AnaProf {
  AnaProf() { }
};

enum class WorkerStatus {
  WORKING,
  STEALING
};

struct WorkerState {
  std::vector<int> graph_idx;
  std::vector<segid_t> start_seg;
  std::vector<segid_t> end_seg;
  std::vector<vertex_t> start_vid;
  std::vector<vertex_t> end_vid;
  std::vector<segid_t> curr_seg;
  std::vector<vertex_t> curr_vid;
  int curr_idx;
  WorkerStatus status;
};

// contexts for worker thhreads
struct AnalyticsCtx {
  bool use_seg_graph = true;
  const livegraph::timestamp_t read_epoch_id;

  std::vector<livegraph::SegGraph*> seg_graphs;
  std::vector<livegraph::Graph*> lg_graphs;
  std::vector<BackupStore*> propertys;

#ifdef ENABLE_EPOCH
  std::vector<std::shared_ptr<livegraph::EpochGraphReader>> seg_graph_readers;
#else
  std::vector<std::shared_ptr<livegraph::SegTransaction>> seg_graph_readers;
#endif
  std::vector<std::shared_ptr<livegraph::Transaction>> lg_graph_readers;

  std::vector<int> vertex_labels;
  std::vector<int> edge_labels;
  std::vector<int> src_vlabel_idx;
  std::vector<int> dst_vlabel_idx;

  // for Gaia
  std::map<int, int> vlabel2idx;
  std::unordered_map<int, int> elabel2idx;

  std::vector<vertex_t> vertex_nums;
  std::vector<segid_t> segment_nums;

  vertex_t total_vertices = 0;
  segid_t total_segments = 0;

  graph::GraphStore* graph_store;
  graph::RGMapping* rg_map;

  int num_threads;
  std::shared_ptr<Barrier> barrier;
  std::vector<WorkerState> worker_states;

  AnalyticsCtx(int num_threads,
               graph::GraphStore* graph_store_,
               graph::RGMapping* rg_map_,
               std::vector<int> edge_labels,
               uint64_t read_ver)
    : num_threads(num_threads), graph_store(graph_store_), rg_map(rg_map_),
      edge_labels(edge_labels), read_epoch_id(read_ver), worker_states(num_threads) {
    barrier = std::make_shared<Barrier>(num_threads);

    // init vertex_labels
    for(int eidx = 0; eidx < edge_labels.size(); eidx++) {
        auto elabel = edge_labels[eidx];
        elabel2idx[elabel] = eidx;
        auto meta = rg_map->get_edge_meta(elabel);
        if(!vlabel2idx.count(meta.src_vlabel)) {
            int vidx = vlabel2idx.size();
            vlabel2idx[meta.src_vlabel] = vidx;
        }

        if(!vlabel2idx.count(meta.dst_vlabel)) {
            int vidx = vlabel2idx.size();
            vlabel2idx[meta.dst_vlabel] = vidx;
        }

        src_vlabel_idx.push_back(vlabel2idx[meta.src_vlabel]);
        dst_vlabel_idx.push_back(vlabel2idx[meta.dst_vlabel]);
        if(meta.undirected) {
            src_vlabel_idx.push_back(vlabel2idx[meta.dst_vlabel]);
            dst_vlabel_idx.push_back(vlabel2idx[meta.src_vlabel]);
        }
    }

    vertex_labels.resize(vlabel2idx.size());
    seg_graphs.resize(vlabel2idx.size(), nullptr);
    lg_graphs.resize(vlabel2idx.size(), nullptr);
    propertys.resize(vlabel2idx.size(), nullptr);
    seg_graph_readers.resize(vlabel2idx.size(), nullptr);
    lg_graph_readers.resize(vlabel2idx.size(), nullptr);
    vertex_nums.resize(vlabel2idx.size());
    segment_nums.resize(vlabel2idx.size());

    for(auto [vlabel, idx] : vlabel2idx) {
        vertex_labels[idx] = vlabel;
        propertys[idx] = graph_store->get_property(vlabel);
        if(config.isUseSegGraph()) {
            seg_graphs[idx] = graph_store->get_graph<livegraph::SegGraph>(vlabel);
#ifdef ENABLE_EPOCH
            seg_graph_readers[idx] = std::make_shared<livegraph::EpochGraphReader>(std::move(seg_graphs[idx]->create_graph_reader(read_epoch_id)));
#else
            seg_graph_readers[idx] = std::make_shared<livegraph::SegTransaction>(std::move(seg_graphs[idx]->create_graph_reader(read_epoch_id)));
#endif
            vertex_t max_vertex_id = seg_graphs[idx]->get_max_vertex_id();
            segid_t max_seg_id = seg_graphs[idx]->get_vertex_seg_id(max_vertex_id);
            vertex_nums[idx] = max_vertex_id;
            segment_nums[idx] = max_seg_id;
            total_vertices += max_vertex_id;
            total_segments += max_seg_id;
        } else {
            lg_graphs[idx] = graph_store->get_graph<livegraph::Graph>(vlabel);
            lg_graph_readers[idx] = std::make_shared<livegraph::Transaction>(
                std::move(lg_graphs[idx]->create_graph_reader(read_epoch_id)));
            vertex_t max_vertex_id = lg_graphs[idx]->get_max_vertex_id();
            vertex_nums[idx] = max_vertex_id;
            total_vertices += max_vertex_id;
        }
    }

    // assert(edge_labels.size() == vlabel2idx.size());

    #if DEBUG_LOG
        std::cout << "Total vertices: " << total_vertices << std::endl;
    #endif
    partition_workload();
  }

  inline void partition_workload() {
    // TODO: fix workload
    // partition task set
    if(config.isUseSegGraph()) {
        segid_t workload = total_segments / num_threads;
        use_seg_graph = true;
        int cur_graph_idx = 0;
        segid_t cur_seg_idx = 0;
        for (int i = 0; i < num_threads; i++) {
            if (i == num_threads - 1) {
                while(cur_graph_idx < edge_labels.size()) {
                    set_start_seg(i, cur_graph_idx, cur_seg_idx);
                    set_end_seg(i, cur_graph_idx, segment_nums[src_vlabel_idx[cur_graph_idx]]+1);
                    cur_seg_idx = 0;
                    cur_graph_idx++;
                }
                this->worker_states[i].curr_seg.resize(this->worker_states[i].graph_idx.size());
                break;
            }
            segid_t local_workload = workload;
            while(local_workload > 0 && cur_graph_idx < edge_labels.size()) {
                set_start_seg(i, cur_graph_idx, cur_seg_idx);
                if((segment_nums[src_vlabel_idx[cur_graph_idx]] - cur_seg_idx) > local_workload) {
                    cur_seg_idx += local_workload;
                    set_end_seg(i, cur_graph_idx, cur_seg_idx);
                    break;
                } else {
                    local_workload -= (segment_nums[src_vlabel_idx[cur_graph_idx]] - cur_seg_idx);
                    cur_seg_idx = 0;
                    set_end_seg(i, cur_graph_idx, segment_nums[src_vlabel_idx[cur_graph_idx]]+1);
                    cur_graph_idx++;
                }
            }
            this->worker_states[i].curr_seg.resize(this->worker_states[i].graph_idx.size());
        }
    } else {
        use_seg_graph = false;
        size_t basic_chunk = 64;
        vertex_t workload = total_vertices / (num_threads * basic_chunk) * basic_chunk;
        int cur_graph_idx = 0;
        vertex_t cur_v_idx = 0;
        for (int i = 0; i < num_threads; i++) {
            if (i == num_threads - 1) {
                while(cur_graph_idx < edge_labels.size()) {
                    set_start_vid(i, cur_graph_idx, cur_v_idx);
                    set_end_vid(i, cur_graph_idx, vertex_nums[src_vlabel_idx[cur_graph_idx]]);
                    cur_v_idx = 0;
                    cur_graph_idx++;
                }
                this->worker_states[i].curr_vid.resize(this->worker_states[i].graph_idx.size());
                break;
            }

            vertex_t local_workload = workload;
            while(local_workload > 0 && cur_graph_idx < edge_labels.size()) {
                set_start_vid(i, cur_graph_idx, cur_v_idx);
                if((vertex_nums[src_vlabel_idx[cur_graph_idx]] - cur_v_idx) > local_workload) {
                    cur_v_idx += local_workload;
                    set_end_vid(i, cur_graph_idx, cur_v_idx);
                    break;
                } else {
                    local_workload -= (vertex_nums[src_vlabel_idx[cur_graph_idx]] - cur_v_idx);
                    cur_v_idx = 0;
                    set_end_vid(i, cur_graph_idx, vertex_nums[src_vlabel_idx[cur_graph_idx]]);
                    cur_graph_idx++;
                }
            }
            this->worker_states[i].curr_vid.resize(this->worker_states[i].graph_idx.size());
        }
    }

    for (int i = 0; i < num_threads; i++) {
        refresh_state(i);
    }
  }

  inline void set_start_seg(int worker_idx, int graph_idx, segid_t seg) {
    this->worker_states[worker_idx].graph_idx.push_back(graph_idx);
    this->worker_states[worker_idx].start_seg.push_back(seg);
  }

  inline void set_end_seg(int worker_idx, int graph_idx, segid_t seg) {
    this->worker_states[worker_idx].end_seg.push_back(seg);
  }

  inline void set_start_vid(int worker_idx, int graph_idx, vertex_t vid) {
    this->worker_states[worker_idx].graph_idx.push_back(graph_idx);
    this->worker_states[worker_idx].start_vid.push_back(vid);
  }

  inline void set_end_vid(int worker_idx, int graph_idx, vertex_t vid) {
    this->worker_states[worker_idx].end_vid.push_back(vid);
  }

  inline void refresh_state(int worker_idx) {
    this->worker_states[worker_idx].curr_idx = 0;
    for(int i = 0; i < this->worker_states[worker_idx].graph_idx.size(); i++) {
        if(use_seg_graph) {
            this->worker_states[worker_idx].curr_seg[i] = this->worker_states[worker_idx].start_seg[i];
        } else {
            this->worker_states[worker_idx].curr_vid[i] = this->worker_states[worker_idx].start_vid[i];
        }
    }
    this->worker_states[worker_idx].status = WorkerStatus::WORKING;
  }

};

/* Registerered Txn execution function */
class AnalyticsWorker;
typedef bool (*ana_fn_t) (AnalyticsWorker *, const std::string& params);

struct AnaDesc {
  std::string name;
  ana_fn_t fn;
  std::string params;
  util::Breakdown_Timer latency_timer; // calculate the latency for each analyics task

  AnaDesc() : name(""), fn(nullptr) {}
  AnaDesc(const std::string &name, ana_fn_t fn)
    : name(name), fn(fn) { }
  AnaDesc(const std::string &name, ana_fn_t fn, std::string& params)
    : name(name), fn(fn), params(params) { }
};

// worker_idx, ctx
typedef void AnaTaskFn (int, void *);

class SubAnalyticsWorker : public ndb_thread {
 public:
  SubAnalyticsWorker(int worker_id, int num_thread);

  void run() override;

  void set_task(AnaTaskFn *task, void *args);

  void clear();  // join

 private:
  const int worker_id_;
  const int thread_id_;
  const int num_thread_;
 
  AnaTaskFn *volatile task_;
  void *  volatile args_;
  volatile bool complete_;
};

/**
 * Graph Analytics Worker
 */
class AnalyticsWorker : public ndb_thread {
 public:
  /* methods */
  AnalyticsWorker(int worker_id, 
                  int num_thread, 
                  uint32_t seed, 
                  graph::GraphStore* graph_store,
                  graph::RGMapping* rg_map);
  
  void run() override;

  template <class Ctx>
  void parallel_process(Ctx& ctx, AnaTaskFn* task);
  
  const AnaProf &get_profile() const { return prof_; }

  inline uint64_t get_read_ver_() const {
    // return LogWorker::get_read_epoch(); 
    return using_ver_;  // for GC
  }

  virtual std::vector<AnaDesc> get_workload() const = 0;

  virtual void thread_local_init() { };

  void master_exit();

  inline int get_worker_id() const { return worker_id_; }

  volatile bool   running;
  volatile bool   inited;
  volatile bool   started;
  util::Breakdown_Timer latency_timer_;

  static struct timespec start_time;

 protected:
  const int worker_id_;
  const int num_thread_;
  const int thread_id_;
  const bool use_seg_graph_;

#ifdef WITH_GRAPE
  // for grape engine
  grape::CommSpec comm_spec_;
  grape::ParallelEngineSpec mt_spec_;
#endif

  // for gemini engine
  std::vector<SubAnalyticsWorker*> subs_;

  graph::GraphStore* graph_store_;
  graph::RGMapping* rg_map_;

  void master_init_();
  void master_process_();

 private:

  AnaProf prof_;
  util::fast_random rand_generator_;

  uint64_t using_ver_;
  int qid_;
  int iter_;
  util::Breakdown_Timer iter_timer_; // calculate the latency for each Qry
};


template <class Ctx>
void AnalyticsWorker::parallel_process(Ctx& ctx, AnaTaskFn* task) {
  // start analytics work, skip itself
  for (int i = 1; i < subs_.size(); ++i) {
      subs_[i]->set_task(task, (void*)&ctx);
  }
  
  task(0, (void *) &ctx);

  for (int i = 1; i < subs_.size(); ++i)
    subs_[i]->clear();
}

// process vertices
template<typename Ctx, typename R>
R process_vertices(int worker_idx,
                   Ctx* ctx,
                   std::function<R(label_t vlabel, vertex_t vtx)> vertex_func,
                   std::vector<Bitmap*>& active_bitmaps)
{
    bool use_seg_graph = ctx->use_seg_graph;
    auto& worker_states = ctx->worker_states;
    auto& local_worker_state = worker_states[worker_idx];
    int num_worker = worker_states.size();
    R reducer = 0;
    size_t basic_chunk = 64;

    ctx->refresh_state(worker_idx);

    for(int g_i = 0; g_i < local_worker_state.graph_idx.size(); g_i++) {
        int graph_idx = local_worker_state.graph_idx[g_i];
        int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
        auto seg_graph = ctx->seg_graphs[vlabel_idx];
        auto lg_graph = ctx->lg_graphs[vlabel_idx];

        auto active = active_bitmaps[vlabel_idx];

        vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];

        // handle work owned by self 
        while (true) {
            if(use_seg_graph) {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_idx].curr_seg[g_i], 1);
                if (seg_id >= worker_states[worker_idx].end_seg[g_i]) break;
                vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                if (v_end > max_vertex_id) {
                    v_end = max_vertex_id;
                }
                for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                    vertex_t local_v_i = v_i;
                    unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            reducer += vertex_func(vlabel_idx, local_v_i);
                        }
                        local_v_i++;
                        word = word >> 1;
                    }
                }
            } else {
                vertex_t v_i = __sync_fetch_and_add(&worker_states[worker_idx].curr_vid[g_i], basic_chunk);
                if (v_i >= worker_states[worker_idx].end_vid[g_i]) break;
                // if(active->get_bit(v_i)) {
                //     reducer += vertex_func(v_i);
                // }
                unsigned long word = active->data[WORD_OFFSET(v_i)];
                while (word != 0) {
                    if (word & 1) {
                        reducer += vertex_func(vlabel_idx, v_i);
                    }
                    v_i++;
                    word = word >> 1;
                }
            }
        }
    }

    // handle work owned by others
    worker_states[worker_idx].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker; t_offset++) {
        int t_i = (worker_idx + t_offset) % num_worker;
        for(int g_i = 0; g_i < worker_states[t_i].graph_idx.size(); g_i++) {
            int graph_idx = worker_states[t_i].graph_idx[g_i];
            int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
            auto seg_graph = ctx->seg_graphs[vlabel_idx];
            auto lg_graph = ctx->lg_graphs[vlabel_idx];

            auto active = active_bitmaps[vlabel_idx];

            vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];
            while (worker_states[t_i].status != WorkerStatus::STEALING) {
                if(use_seg_graph) {
                    segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg[g_i], 1);
                    if (seg_id >= worker_states[t_i].end_seg[g_i]) break;
                    vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                    if (v_end > max_vertex_id) {
                        v_end = max_vertex_id;
                    }
                    for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                        vertex_t local_v_i = v_i;
                        unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                        while (word != 0) {
                            if (word & 1) {
                                reducer += vertex_func(vlabel_idx, local_v_i);
                            }
                            local_v_i++;
                            word = word >> 1;
                        }
                    }
                } else {
                    vertex_t v_i = __sync_fetch_and_add(&worker_states[t_i].curr_vid[g_i], basic_chunk);
                    if (v_i >= worker_states[t_i].end_vid[g_i]) break;
                    unsigned long word = active->data[WORD_OFFSET(v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            reducer += vertex_func(vlabel_idx, v_i);
                        }
                        v_i++;
                        word = word >> 1;
                    }
                }
            }
        }
    }
    return reducer;
}

// process edges
template<typename Ctx, typename R>
R process_edges_push(int worker_idx, 
                     Ctx* ctx, 
                     std::function<R(label_t, label_t, vertex_t, EdgeIteratorBase&)> push_edge_func, 
                     std::vector<Bitmap*>& active_bitmaps) {
    auto& worker_states = ctx->worker_states;
    auto& local_worker_state = worker_states[worker_idx];
    int num_worker = worker_states.size();
    bool use_seg_graph = ctx->use_seg_graph;
    R reducer = 0;
    size_t basic_chunk = 64;

    ctx->refresh_state(worker_idx);

    for(int g_i = 0; g_i < local_worker_state.graph_idx.size(); g_i++) {
        int graph_idx = local_worker_state.graph_idx[g_i];
        int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
        auto seg_graph = ctx->seg_graphs[vlabel_idx];
        auto lg_graph = ctx->lg_graphs[vlabel_idx];
        auto& seg_txn = ctx->seg_graph_readers[vlabel_idx];
        auto& lg_txn = ctx->lg_graph_readers[vlabel_idx];

        label_t edge_label = ctx->edge_labels[graph_idx];
        size_t edge_prop_size = ctx->rg_map->get_edge_meta(static_cast<int>(edge_label)).edge_prop_size;

        vertex_t src_vlabel = ctx->src_vlabel_idx[graph_idx];
        vertex_t dst_vlabel = ctx->dst_vlabel_idx[graph_idx];

        vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];

        auto active = active_bitmaps[src_vlabel];

        if(DEBUG_LOG) {
            if(use_seg_graph) {
                std::cout << "Analytics thread [" << worker_idx << "]" << " graph idx:"
                    << worker_states[worker_idx].graph_idx[g_i] << ", start seg:"
                    << worker_states[worker_idx].start_seg[g_i] << ", end seg:"
                    << worker_states[worker_idx].end_seg[g_i] << std::endl;
            } else {
                std::cout << "Analytics thread [" << worker_idx << "]" << " graph idx:"
                    << worker_states[worker_idx].graph_idx[g_i] <<", start vertex:"
                    << worker_states[worker_idx].start_vid[g_i] << ", end vertex:"
                    << worker_states[worker_idx].end_vid[g_i] << std::endl;
            }
        }

        // handle work owned by self
        while (true) {
            if(use_seg_graph) {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_idx].curr_seg[g_i], 1);
                if (seg_id >= worker_states[worker_idx].end_seg[g_i]) break;
                auto segment = seg_txn->locate_segment(seg_id, edge_label);
                vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                if (v_end > max_vertex_id) {
                    v_end = max_vertex_id;
                }
                for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                    // TODO: use outgoing_adj_bitmap to avoid empty computation
                    vertex_t local_v_i = v_i;
                    unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            auto edge_iter = seg_txn->get_edges_in_seg(segment, local_v_i, edge_prop_size);
                            reducer += push_edge_func(src_vlabel, dst_vlabel, local_v_i, edge_iter);
                        }
                        local_v_i++;
                        word = word >> 1;
                    }
                }
            } else {
                vertex_t v_i = __sync_fetch_and_add(&worker_states[worker_idx].curr_vid[g_i], basic_chunk);
                if (v_i >= worker_states[worker_idx].end_vid[g_i]) break;
                // if(active->get_bit(v_i)) {
                //     auto edge_iter = lg_txn->get_edges(v_i, edge_label, false);
                //     reducer += push_edge_func(v_i, edge_iter);
                // }
                unsigned long word = active->data[WORD_OFFSET(v_i)];
                while (word != 0) {
                    if (word & 1) {
                        auto edge_iter = lg_txn->get_edges(v_i, edge_label, false);
                        reducer += push_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                    }
                    v_i++;
                    word = word >> 1;
                }
            }
        }
    }

    // handle work owned by others 
    worker_states[worker_idx].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker; t_offset++) {
        int t_i = (worker_idx + t_offset) % num_worker;
        for(int g_i = 0; g_i < worker_states[t_i].graph_idx.size(); g_i++) {
            int graph_idx = worker_states[t_i].graph_idx[g_i];
            int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
            auto seg_graph = ctx->seg_graphs[vlabel_idx];
            auto lg_graph = ctx->lg_graphs[vlabel_idx];
            auto& seg_txn = ctx->seg_graph_readers[vlabel_idx];
            auto& lg_txn = ctx->lg_graph_readers[vlabel_idx];

            label_t edge_label = ctx->edge_labels[graph_idx];
            size_t edge_prop_size = ctx->rg_map->get_edge_meta(static_cast<int>(edge_label)).edge_prop_size;

            vertex_t src_vlabel = ctx->src_vlabel_idx[graph_idx];
            vertex_t dst_vlabel = ctx->dst_vlabel_idx[graph_idx];

            vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];

            auto active = active_bitmaps[src_vlabel];

            while (worker_states[t_i].status != WorkerStatus::STEALING) {
                if(use_seg_graph) {
                    segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg[g_i], 1);
                    if (seg_id >= worker_states[t_i].end_seg[g_i]) break;
                    auto segment = seg_txn->locate_segment(seg_id, edge_label);
                    vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                    if (v_end > max_vertex_id) {
                        v_end = max_vertex_id;
                    }
                    for(vertex_t v_i = v_start; v_i < v_end; v_i += basic_chunk) {
                        // TODO: use outgoing_adj_bitmap to avoid empty computation
                        vertex_t local_v_i = v_i;
                        unsigned long word = active->data[WORD_OFFSET(local_v_i)];
                        while (word != 0) {
                            if (word & 1) {
                                auto edge_iter = seg_txn->get_edges_in_seg(segment, local_v_i, edge_prop_size);
                                reducer += push_edge_func(src_vlabel, dst_vlabel, local_v_i, edge_iter);
                            }
                            local_v_i++;
                            word = word >> 1;
                        }
                    }
                } else {
                    vertex_t v_i = __sync_fetch_and_add(&worker_states[t_i].curr_vid[g_i], basic_chunk);
                    if (v_i >= worker_states[t_i].end_vid[g_i]) break;
                    unsigned long word = active->data[WORD_OFFSET(v_i)];
                    while (word != 0) {
                        if (word & 1) {
                            auto edge_iter = lg_txn->get_edges(v_i, edge_label, false);
                            reducer += push_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                        }
                        v_i++;
                        word = word >> 1;
                    }
                }
            }
        }
    }

    return reducer;
}


template<typename Ctx, typename R>
R process_edges_pull(int worker_idx, 
                     Ctx* ctx, 
                     std::function<R(label_t, label_t, vertex_t, EdgeIteratorBase&)> pull_edge_func, 
                     std::vector<Bitmap*>& active_bitmaps) {
    auto& worker_states = ctx->worker_states;
    auto& local_worker_state = worker_states[worker_idx];
    int num_worker = worker_states.size();
    bool use_seg_graph = ctx->use_seg_graph;
    R reducer = 0;
    size_t basic_chunk = 64;

    ctx->refresh_state(worker_idx);

    for(int g_i = 0; g_i < local_worker_state.graph_idx.size(); g_i++) {
        int graph_idx = local_worker_state.graph_idx[g_i];
        int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
        auto seg_graph = ctx->seg_graphs[vlabel_idx];
        auto lg_graph = ctx->lg_graphs[vlabel_idx];
        auto& seg_txn = ctx->seg_graph_readers[vlabel_idx];
        auto& lg_txn = ctx->lg_graph_readers[vlabel_idx];

        vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];

        label_t edge_label = ctx->edge_labels[graph_idx];
        size_t edge_prop_size = ctx->rg_map->get_edge_meta(static_cast<int>(edge_label)).edge_prop_size;

        vertex_t src_vlabel = ctx->src_vlabel_idx[graph_idx];
        vertex_t dst_vlabel = ctx->dst_vlabel_idx[graph_idx];

        // handle work owned by self 
        while (true) {
            if(use_seg_graph) {
                segid_t seg_id = __sync_fetch_and_add(&worker_states[worker_idx].curr_seg[g_i], 1);
                if (seg_id >= worker_states[worker_idx].end_seg[g_i]) break;
                auto segment = seg_txn->locate_segment(seg_id, edge_label);
                vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                if (v_end > max_vertex_id) {
                    v_end = max_vertex_id;
                }
                for (vertex_t v_i = v_start; v_i < v_end; v_i++) {
                    // vertex_t v_i = compressed_incoming_adj_index[p_v_i].vertex;
                    auto edge_iter = seg_txn->get_edges_in_seg(segment, v_i, edge_prop_size);
                    reducer += pull_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                }
            } else {
                vertex_t v_start = __sync_fetch_and_add(&worker_states[worker_idx].curr_vid[g_i], basic_chunk);
                if (v_start >= worker_states[worker_idx].end_vid[g_i]) break;
                vertex_t v_end = v_start + basic_chunk;
                if(v_end > worker_states[worker_idx].end_vid[g_i]) {
                    v_end = worker_states[worker_idx].end_vid[g_i];
                }
                for(vertex_t v_i = v_start; v_i < v_end; v_i++) {
                    auto edge_iter = lg_txn->get_edges(v_i, edge_label);
                    reducer += pull_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                }
            }
        }
    }

    // handle work owned by others 
    worker_states[worker_idx].status = WorkerStatus::STEALING;
    for (int t_offset = 1; t_offset < num_worker; t_offset++) {
        int t_i = (worker_idx + t_offset) % num_worker;
        for(int g_i = 0; g_i < worker_states[t_i].graph_idx.size(); g_i++) {
            int graph_idx = worker_states[t_i].graph_idx[g_i];
            int vlabel_idx = ctx->src_vlabel_idx[graph_idx];
            auto seg_graph = ctx->seg_graphs[vlabel_idx];
            auto lg_graph = ctx->lg_graphs[vlabel_idx];
            auto& seg_txn = ctx->seg_graph_readers[vlabel_idx];
            auto& lg_txn = ctx->lg_graph_readers[vlabel_idx];

            label_t edge_label = ctx->edge_labels[graph_idx];
            size_t edge_prop_size = ctx->rg_map->get_edge_meta(static_cast<int>(edge_label)).edge_prop_size;

            vertex_t src_vlabel = ctx->src_vlabel_idx[graph_idx];
            vertex_t dst_vlabel = ctx->dst_vlabel_idx[graph_idx];

            vertex_t max_vertex_id = ctx->vertex_nums[vlabel_idx];

            while (worker_states[t_i].status != WorkerStatus::STEALING) {
                if(use_seg_graph) {
                    segid_t seg_id = __sync_fetch_and_add(&worker_states[t_i].curr_seg[g_i], 1);
                    if (seg_id >= worker_states[t_i].end_seg[g_i]) break;
                    auto segment = seg_txn->locate_segment(seg_id, edge_label);
                    vertex_t v_start = seg_graph->get_seg_start_vid(seg_id), v_end = seg_graph->get_seg_end_vid(seg_id);
                    if (v_end > max_vertex_id) {
                        v_end = max_vertex_id;
                    }
                    for (vertex_t v_i = v_start; v_i < v_end; v_i++) {
                        // vertex_t v_i = compressed_incoming_adj_index[p_v_i].vertex;
                        auto edge_iter = seg_txn->get_edges_in_seg(segment, v_i, edge_prop_size);
                        reducer += pull_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                    }
                } else {
                    vertex_t v_start = __sync_fetch_and_add(&worker_states[t_i].curr_vid[g_i], basic_chunk);
                    if (v_start >= worker_states[t_i].end_vid[g_i]) break;
                    vertex_t v_end = v_start + basic_chunk;
                    if(v_end > worker_states[t_i].end_vid[g_i]) {
                        v_end = worker_states[t_i].end_vid[g_i];
                    }
                    for(vertex_t v_i = v_start; v_i < v_end; v_i++) {
                        auto edge_iter = lg_txn->get_edges(v_i, edge_label, true);
                        reducer += pull_edge_func(src_vlabel, dst_vlabel, v_i, edge_iter);
                    }
                }
            }
        }
    }

    return reducer;
}


} // namespace oltp
} // namespace nocc

#endif
