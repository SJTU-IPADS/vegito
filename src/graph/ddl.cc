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
#include "ddl.h"

using namespace std;

namespace nocc {
namespace graph {

RGMapping::RGMapping(int p_id) 
  : edges_(MAX_ELABELS), p_id_(p_id) {
  for (int i = 0; i < MAX_TABLES; ++i) {
    table2graph[i] = NO_EXIST; 
    key2vids_lock_[i] = 0; 
  }
  for (int i = 0; i < MAX_VLABELS; ++i) {
    graph2table[i] = NO_EXIST; 
  }

  for (int i = 0; i < MAX_TABLES; ++i) {
    for (int j = 0; j < MAX_COLS; ++j) {
      col2vprop_[i][j] = NO_EXIST;
    }
  }

  for (EdgeMeta &meta : edges_) {
    meta.src_vlabel = meta.dst_vlabel = NO_EXIST;
  }
}

void RGMapping::define_vertex(int vertex_label, int table_id) {
  assert(vertex_label < MAX_TABLES);
  table2graph[table_id] = vertex_label;
  graph2table[vertex_label] = table_id;
}

void RGMapping::add_vprop_mapping(int vertex_label, int vprop_id, int col_id) {
  assert(col_id < MAX_COLS);

  col2vprop_[vertex_label][col_id] = vprop_id;
  vprop2col_[vertex_label][vprop_id] = col_id;
}

int RGMapping::get_vprop2col(int vertex_label, int vprop_id) {
  if(vprop2col_[vertex_label].count(vprop_id)) {
    return vprop2col_[vertex_label][vprop_id];
  } else {
    return -1;
  }
}

// one to many
void RGMapping::define_1n_edge(int edge_label, 
                            int src_vlabel, int dst_vlabel, int fk_col,
                            bool undirected, size_t edge_prop_size) {
  assert(edge_label < MAX_ELABELS);  

  EdgeMeta &meta = edges_[edge_label];
  meta.src_vlabel = src_vlabel;
  meta.dst_vlabel = dst_vlabel;
  meta.src_fk_col = fk_col;
  meta.dst_fk_col = NO_EXIST;  // XXX: only support one direct
  meta.undirected = undirected;
  meta.edge_prop_size = edge_prop_size;

  vlabel2elabel_[{src_vlabel, dst_vlabel}] = edge_label;
}

void RGMapping::define_nn_edge(int edge_label, int src_vlabel, int dst_vlabel,
                            int src_fk_col, int dst_fk_col,
                            bool undirected, size_t edge_prop_size) {
  assert(edge_label < MAX_ELABELS);  

  EdgeMeta &meta = edges_[edge_label];
  meta.src_vlabel = src_vlabel;
  meta.dst_vlabel = dst_vlabel;
  meta.src_fk_col = src_fk_col;
  meta.dst_fk_col = dst_fk_col;
  meta.undirected = undirected;
  meta.edge_prop_size = edge_prop_size;

  vlabel2elabel_[{src_vlabel, dst_vlabel}] = edge_label;
}
  
void RGMapping::add_eprop(int edge_label, int prop_id, int col_id) {
  // TODO
}



}  // namespace graph
}  // namespace nocc
