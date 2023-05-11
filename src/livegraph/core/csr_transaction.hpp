/* Copyright 2020 Guanyu Feng, Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <vector>
#include "csr_graph.hpp"
#include "edge_iterator.hpp"

namespace livegraph
{
    class CSRTransaction
    {
    public:
        CSRTransaction(CSRGraph &_graph) : graph(_graph), valid(true) {}

        ~CSRTransaction()
        {
            if (valid)
               valid = false;
        }

        CSREdgeIterator get_edges(vertex_t src, label_t label) {
            if(label >= graph.vertices.size() || src >= graph.vertices[label].size())
                return CSREdgeIterator(graph.edges[label], 0, 0);
            off_t end = src + 1 < graph.vertices[label].size() ? graph.vertices[label][src + 1] : graph.edges[label].size();
            return CSREdgeIterator(graph.edges[label], graph.vertices[label][src], end);
        }
        
    private:
        CSRGraph &graph;
        bool valid;
        
        void pagerank();
        void bfs();
        void sssp();
        void cc();
    };
} // namespace livegraph
