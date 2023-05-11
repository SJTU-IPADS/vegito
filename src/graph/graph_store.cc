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

#include <cassert>
#include <vector>
#include <unordered_map>

#include "graph_store.h"

using namespace nocc::util;

namespace nocc {
namespace graph {

template <>
livegraph::SegGraph* GraphStore::get_graph<livegraph::SegGraph>(uint64_t vlabel)
{
  return seg_graphs_[vlabel];
}

template <>
livegraph::Graph* GraphStore::get_graph<livegraph::Graph>(uint64_t vlabel)
{
  return live_graphs_[vlabel];
}

template <class GraphType>
GraphType* GraphStore::get_graph(uint64_t vlabel)
{
  return nullptr;
}

}  // namespace graph
}  // namespace nocc
