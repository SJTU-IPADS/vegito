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

#pragma once

#include <cassert>
#include <vector>
#include <map>
#include <unordered_map>
#include <tbb/concurrent_unordered_map.h>

#include "util/util.h"

using namespace nocc::util;

#define USE_TBB_MAP 1

namespace nocc {
namespace graph {

struct EdgeMeta {
  int src_vlabel;
  int dst_vlabel;
  int src_fk_col;  // col_id of source node keys (e.g., OL_O_ID in ORLI)
  int dst_fk_col;  // only used in many-to-many
  size_t edge_prop_size = 0;
  bool undirected = false;
};

class RGMapping {

 public:
  RGMapping(int p_id);

  /* VERTEX MAPPING */

  // define vertex label (table_id -> vertex_label)
  void define_vertex(int vertex_label, int table_id);

  // define vertex property
  void add_vprop_mapping(int vertex_label, int prop_id, int col_id);
  int get_vprop2col(int vertex_label, int vprop_id);
  
  /* EDGE MAPPING */
  
  // one to many
  void define_1n_edge(int edge_label, int src_vlabel, int dst_vlabel, int fk_col,
                      bool undirected = false, size_t edge_prop_size = 0);

  // many to many
  void define_nn_edge(int edge_label, int src_vlabel, int dst_vlabel,
                   int src_fk_col, int dst_fk_col, bool undirected = false, size_t edge_prop_size = 0);

  // define edge property
  void add_eprop(int edge_label, int prop_id, int col_id);

  inline int getPID() const {
    return p_id_;
  }

  // get vertex mapping
  inline int get_vlabel(int table_id) const {
    return table2graph[table_id];
  }

  inline int get_table(int vlabel) const {
    return graph2table[vlabel];
  }

  inline const std::vector<EdgeMeta> &get_edge_metas() const {
    return edges_;
  }

  inline int get_elabel_from_vlabel(int src_vlabel, int dst_vlabel) {
    return vlabel2elabel_[{src_vlabel, dst_vlabel}];
  }

  inline const EdgeMeta &get_edge_meta(int elabel) const {
    assert(elabel < MAX_ELABELS);
    return edges_[elabel];
  }

  // return edge_label
  inline int get_edge_meta(int dst_vlabel, EdgeMeta &meta) const {
    for (int i = 0; i < edges_.size(); ++i) {
      const EdgeMeta &m = edges_[i];
      if (m.dst_vlabel == dst_vlabel) {
        meta = m;
        return i;
      }
    }
    return NO_EXIST;
  }

#if USE_TBB_MAP
  inline void set_key2vid(int table_id, uint64_t key, uint64_t vid) {
    assert(table_id < MAX_TABLES);
    key2vids_[table_id].insert({key, vid});
    vid2keys_[table_id].insert({vid, key});
  } 
  
  inline uint64_t get_key2vid(int table_id, uint64_t key) const {
    uint64_t ret;
    auto got = key2vids_[table_id].find(key);
    // assert(got != key2vids_[table_id].end());
    if (got != key2vids_[table_id].end()) {
      ret = got->second;
      return ret;
    } else return UINT64_MAX;
  }

  inline uint64_t get_vid2key(int table_id, uint64_t vid) const {
    uint64_t ret;
    auto got = vid2keys_[table_id].find(vid);
    if(got == vid2keys_[table_id].end())
      ret = 0;
    else ret = got->second;
    return ret;
  }
#else
  inline void set_key2vid(int table_id, uint64_t key, uint64_t vid) {
    assert(table_id < MAX_TABLES);
    lock32(&key2vids_lock_[table_id]);
    key2vids_[table_id][key] = vid;
    vid2keys_[table_id][vid] = key;
    unlock32(&key2vids_lock_[table_id]);
  } 
  
  inline uint64_t get_key2vid(int table_id, uint64_t key) const {
    uint64_t ret;
    lock32(&key2vids_lock_[table_id]);
    std::unordered_map<uint64_t, uint64_t>::const_iterator 
      got = key2vids_[table_id].find(key);
    assert(got != key2vids_[table_id].end());
    ret = got->second;
    unlock32(&key2vids_lock_[table_id]);
    return ret;
  }

  inline uint64_t get_vid2key(int table_id, uint64_t vid) const {
    uint64_t ret;
    lock32(&key2vids_lock_[table_id]);
    std::unordered_map<uint64_t, uint64_t>::const_iterator 
      got = vid2keys_[table_id].find(vid);
    if(got == vid2keys_[table_id].end())
      ret = 0;
    else ret = got->second;
    unlock32(&key2vids_lock_[table_id]);
    return ret;
  }
#endif

 static const int NO_EXIST = -1;

 private:
  static const int MAX_TABLES = 30;
  static const int MAX_COLS = 10;
  static const int MAX_VPROPS = 10;
  static const int MAX_VLABELS = 30;
  static const int MAX_ELABELS = MAX_VLABELS + 30;

  const int p_id_;

  int table2graph[MAX_TABLES];
  int graph2table[MAX_VLABELS];

  // <vlabel> -> [<vprop id> -> <tp col>]
  std::unordered_map<int, std::unordered_map<int, int>> vprop2col_;
  // <vlabel> -> [<tp col> -> <vprop id>]
  std::unordered_map<int, std::unordered_map<int, int>> col2vprop_;
  
  std::vector<EdgeMeta> edges_;

  // <src_vlabel, dst_vlabel> -> elabel
  std::map<std::pair<int, int>, int> vlabel2elabel_;

  // TODO: this is tmp, since the vid is not corresponding to offset now
  mutable uint32_t key2vids_lock_[MAX_TABLES];

#if USE_TBB_MAP
  tbb::concurrent_unordered_map<uint64_t, uint64_t> key2vids_[MAX_TABLES];
  tbb::concurrent_unordered_map<uint64_t, uint64_t> vid2keys_[MAX_TABLES];
#else
  std::unordered_map<uint64_t, uint64_t> key2vids_[MAX_TABLES];
  std::unordered_map<uint64_t, uint64_t> vid2keys_[MAX_TABLES];
#endif
};

}  // namespace graph
}  // namespace nocc
