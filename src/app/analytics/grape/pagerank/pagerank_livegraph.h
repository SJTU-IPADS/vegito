/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_PAGERANK_LIVEGRAPH_H_
#define EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_PAGERANK_LIVEGRAPH_H_

#include <grape/grape.h>

#include "pagerank_livegraph_context.h"

namespace grape {

/**
 * @brief An implementation of PageRank, the version in LDBC, which can work
 * on both directed and undirected graphs.
 *
 * This version of PageRank inherits ParallelAppBase. Messages can be sent in
 * parallel with the evaluation process. This strategy improves performance by
 * overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */

template <typename FRAG_T>
class PageRankLiveGraph
    : public ParallelAppBase<FRAG_T, PageRankLiveGraphContext<FRAG_T>>,
      public Communicator,
      public ParallelEngine {
 public:
  using vertex_t = typename FRAG_T::vertex_t;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr bool need_split_edges = true;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(PageRankLiveGraph<FRAG_T>,
                          PageRankLiveGraphContext<FRAG_T>, FRAG_T)

  PageRankLiveGraph() {}
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto vertices = frag.Vertices();

    size_t graph_vnum = frag.GetVerticesNum();
    messages.InitChannels(thread_num());

    ctx.step = 0;
    double p = 1.0 / graph_vnum;

    // assign initial ranks
    ForEach(vertices, [&ctx, &frag, p, &messages](int tid, vertex_t u) {
      auto edgeIterator = frag.GetEdgeIterator(u);
      int EdgeNum = 0;
      while(edgeIterator.valid()) {
          EdgeNum++;
          edgeIterator.next();
      }
      //int EdgeNum = edgeIterator.size();
      // int EdgeNum = frag.GetOutgoingAdjList(u).Size();
      ctx.degree[u] = EdgeNum;
      if (EdgeNum > 0) {
        ctx.result[u] = p / EdgeNum;
      } else {
        ctx.result[u] = p;
      }
    });

    for (auto u : vertices) {
      if (ctx.degree[u] == 0) {
        ++ctx.dangling_vnum;
      }
    }

    double dangling_sum = p * static_cast<double>(ctx.dangling_vnum);

    Sum(dangling_sum, ctx.dangling_sum);

    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto vertices = frag.Vertices();
    double dangling_sum = ctx.dangling_sum;

    size_t graph_vnum = frag.GetVerticesNum();

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      auto& degree = ctx.degree;
      auto& result = ctx.result;

      for (auto v : vertices) {
        if (degree[v] != 0) {
          result[v] *= degree[v];
        }
      }
      return;
    }

    double base =
        (1.0 - ctx.delta) / graph_vnum + ctx.delta * dangling_sum / graph_vnum;
    
    // pull ranks from neighbors
    ForEach(vertices, [&ctx, base, &frag](int tid, vertex_t u) {
      if (ctx.degree[u] == 0) {
        ctx.next_result[u] = base;
      } else {
        double cur = 0;
        auto edgeIterator = frag.GetEdgeIterator(u);
        while (edgeIterator.valid()) {
          vertex_t v(edgeIterator.dst_id());
          cur += ctx.result[v];
          edgeIterator.next();
        }
        ctx.next_result[u] = cur;
      }
    });

    ctx.result.Swap(ctx.next_result);

    double new_dangling = base * static_cast<double>(ctx.dangling_vnum);

    Sum(new_dangling, ctx.dangling_sum);

    messages.ForceContinue();
  }
};

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_PAGERANK_LIVEGRAPH_H_
