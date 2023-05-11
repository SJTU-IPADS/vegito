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

#ifndef EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_H_
#define EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_H_

#include <grape/grape.h>
#include <app/ch/ch_schema.h>

#include "gcn_livegraph_context.h"

using namespace nocc;

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
class GCNLiveGraph
    : public ParallelAppBase<FRAG_T, GCNLiveGraphContext<FRAG_T>>,
      public Communicator,
      public ParallelEngine {
 public:
  using vertex_t = typename FRAG_T::vertex_t;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr bool need_split_edges = true;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(GCNLiveGraph<FRAG_T>,
                          GCNLiveGraphContext<FRAG_T>, FRAG_T)

  GCNLiveGraph() {}
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto vertices = frag.Vertices();
    messages.InitChannels(thread_num());

    std::cout<<"This is for GCN ############__"<<std::endl;

    for (auto v : vertices) {
      uint64_t key = ctx.rg_map->get_vid2key(nocc::oltp::ch::ORDE, v.GetValue());
      if(key != 0) {
        // std::cout<<"PR init value #### "<< 
        *(int*)(ctx.backup_store->Get(nocc::oltp::ch::ORDE, key, nocc::oltp::ch::O_PR, 0));
        // std::cout<<"CID init value #### "<<
         *(int*)(ctx.backup_store->Get(nocc::oltp::ch::ORDE, key, nocc::oltp::ch::O_C_ID, 0));
        //std::cout<<"CNT init value #### "<< 
        *(int*)(ctx.backup_store->Get(nocc::oltp::ch::ORDE, key, nocc::oltp::ch::O_OL_CNT, 0));
        //break;      
      }
      else{
        std::cout<<"ERROR detected #####"<<std::endl;
      }
    }

    std::vector<int> property_names(ctx.input_size);
    property_names[0] = nocc::oltp::ch::O_PR;
    property_names[1] = nocc::oltp::ch::O_C_ID;
    property_names[2] = nocc::oltp::ch::O_OL_CNT;

    ForEach(vertices, [&ctx, &frag, property_names](int tid, vertex_t u) {
      // std::string_view v_data = frag.GetVertex(u);
      std::vector<int> resolved_feas(ctx.input_size);
      
      for (size_t i = 0; i < ctx.input_size; i++) {
        // std::bitset<32> bits(v_data.substr(i * 32, 32));
        // resolved_feas[i] = bits.to_ulong();
        resolved_feas[i] = 1;
      }
      
      /*
      auto v_id = u.GetValue();
      uint64_t key = ctx.rg_map->get_vid2key(nocc::oltp::ch::ORDE, v_id);
      for (size_t i = 0; i < ctx.input_size; i++) {
        resolved_feas[i] = *(int*)(ctx.backup_store->Get(nocc::oltp::ch::ORDE, key, property_names[i], 0));
      }
      */
      
      ctx.trans_0[u].resize(ctx.hidden_size);

      for (size_t i = 0; i < ctx.hidden_size; i++) {
        for (size_t j = 0; j < ctx.input_size; j++) {
          //ctx.trans_0[u][i] = 0;
          ctx.trans_0[u][i] += resolved_feas[j] * ctx.weight_0[i][j];
        }
      }
    });

    std::cout<<"PEVal stage 1 ####"<<std::endl;

    ForEach(vertices, [&ctx, &frag](int tid, vertex_t u) {
      ctx.hidden_result[u].resize(ctx.hidden_size);
      auto edgeIterator = frag.GetEdgeIterator(u);
      while (edgeIterator.valid()) {
        vertex_t v(edgeIterator.dst_id());
        for (size_t i = 0; i < ctx.hidden_size; i++) {
          ctx.hidden_result[u][i] += ctx.trans_0[v][i];
        }
        edgeIterator.next();
      }
      for (size_t i = 0; i < ctx.hidden_size; i++) {
        if (ctx.hidden_result[u][i] < 0) {
          ctx.hidden_result[u][i] = 0;
        }
      }
    });

    std::cout<<"PEVal stage 2 ####"<<std::endl;

    
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto vertices = frag.Vertices();
    
    ForEach(vertices, [&ctx, &frag](int tid, vertex_t u) {
      ctx.trans_1[u].resize(ctx.output_size);
      for (size_t i = 0; i < ctx.output_size; i++) {
        for (size_t j = 0; j < ctx.hidden_size; j++) {
          ctx.trans_1[u][i] += ctx.hidden_result[u][j] * ctx.weight_1[i][j];
        }
      }
    });

    std::cout<<"IncEval stage 1 ####"<<std::endl;

    ForEach(vertices, [&ctx, &frag](int tid, vertex_t u) {
      ctx.result[u].resize(ctx.output_size);
      auto edgeIterator = frag.GetEdgeIterator(u);
      while (edgeIterator.valid()) {
        vertex_t v(edgeIterator.dst_id());
        for (size_t i = 0; i < ctx.output_size; i++) {
          ctx.result[u][i] += ctx.trans_1[v][i];
        }
        edgeIterator.next();
      }
      for (size_t i = 0; i < ctx.output_size; i++) {
        if (ctx.result[u][i] < 0) {
          ctx.result[u][i] = 0;
        }
      }
    });

    std::cout<<"IncEval stage 2 ####"<<std::endl;

  }
};

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_H_
