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

#ifndef EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_CONTEXT_H_

#include <grape/grape.h>
#include <backup_store/backup_db.h>
#include <graph/ddl.h>

#include <iomanip>

using namespace nocc;

namespace grape {
/**
 * @brief Context for the auto-parallel version of PageRank.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class GCNLiveGraphContext : public VertexDataContext<FRAG_T, int> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

 public:
  explicit GCNLiveGraphContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, int>(fragment, true) {}

  void Init(ParallelMessageManager& messages, BackupDB* backup_store, nocc::graph::RGMapping* rg_map) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    
    this->rg_map = rg_map;
    this->backup_store = backup_store;


    result.Init(vertices);
    hidden_result.Init(vertices);
    trans_0.Init(vertices);
    trans_1.Init(vertices);
    input_size = 3;
    hidden_size = 128;
    output_size = 16;
    weight_0.resize(hidden_size);
    weight_1.resize(output_size);

    for (size_t i = 0; i < hidden_size; i++) {
      weight_0[i].resize(input_size);
      std::generate(weight_0[i].begin(), weight_0[i].end(), [](){
        return rand();
      });
    }

    for (size_t i = 0; i < output_size; i++) {
      weight_1[i].resize(hidden_size);
      std::generate(weight_1[i].begin(), weight_1[i].end(), [](){
        return rand();
      });
    }


  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    /*
    for (auto v : vertices) {
      os << v.GetValue() << " " << std::scientific << std::setprecision(15)
         << result[v] << std::endl;
    }
    */
  }

  typename FRAG_T::template vertex_array_t<std::vector<int>> result;
  typename FRAG_T::template vertex_array_t<std::vector<int>> hidden_result;
  typename FRAG_T::template vertex_array_t<std::vector<int>> trans_0;
  typename FRAG_T::template vertex_array_t<std::vector<int>> trans_1;
  std::vector<std::vector<int>> weight_0, weight_1;
  int input_size, hidden_size, output_size;

  nocc::graph::RGMapping* rg_map;
  BackupDB* backup_store;

};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LIVEGRAPH_GCN_LIVEGRAPH_CONTEXT_H_
