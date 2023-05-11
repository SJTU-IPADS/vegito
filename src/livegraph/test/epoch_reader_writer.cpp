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

#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "core/livegraph.hpp"

using namespace livegraph;

TEST_CASE("testing the EpochWriter&Reader: new_vertex")
{
    SegGraph graph;

    {
        auto writer = graph.create_graph_writer(0);
        CHECK(writer.new_vertex() == 0);
        CHECK(writer.new_vertex() == 1);
        CHECK(writer.new_vertex() == 2);
        CHECK(writer.new_vertex() == 3);
        for(int i = 0; i < 1000; i++) {
            writer.new_vertex();
        }
    }
    {
        auto writer = graph.create_graph_writer(0);
        writer.put_edge(0, 0, 1);
        writer.put_edge(0, 0, 2);
        writer.put_edge(1, 0, 3);
        writer.put_edge(3, 0, 0);
        writer.put_edge(3, 0, 2);
        for(int dst = 100; dst < 200; dst++) {
            writer.put_edge(0, 0, dst);
        }
    }
    {
        auto writer = graph.create_graph_writer(1);
        writer.put_edge(0, 0, 3);
        writer.put_edge(1, 0, 2);
        writer.put_edge(2, 0, 1);
        writer.put_edge(2, 0, 3);
        writer.put_edge(3, 0, 1);
        for(int dst = 200; dst < 500; dst++) {
            writer.put_edge(0, 0, dst);
        }
    }
    {
        auto reader = graph.create_graph_reader(0);
        auto iter = reader.get_edges(0, 0);
        std::cout << "Epoch[0]" << std::endl << "Vertex 0 neighbors:";
        while(iter.valid()) {
            std::cout << iter.dst_id() << " ";
            iter.next();
        }
        std::cout << std::endl;
    }
    {
        auto reader = graph.create_graph_reader(1);
        auto iter = reader.get_edges(0, 0);
        std::cout << "Epoch[1]" << std::endl << "Vertex 0 neighbors:";
        while(iter.valid()) {
            std::cout << iter.dst_id() << " ";
            iter.next();
        }
        std::cout << std::endl;
    }
}
