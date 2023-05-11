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

#include "replayer.h"

#include <string_view>

using namespace livegraph;
using namespace std;

namespace nocc {
namespace graph {

GraphReplay::GraphReplay(Graph &graph, const BackupDB &db, RGMapping* mapping)
  : graph_(graph), db_(db), mapping_(mapping) {
  
}

// XXX: `ver` is unused
void GraphReplay::Insert(int table_id, uint64_t offset, char *value, 
                         const vector<int> &cols, uint64_t ver,
                         uint64_t fk) {
  Transaction tx = graph_.begin_transaction();
  // 1. insert vertex
  int vlabel = mapping_->get_vlabel(table_id);
  tx.new_vertex_by_id((vertex_t) offset);
  // TODO: put_vertex: put vertex property
  
  // 2. insert edge
  // XXX: insert edge when target vertex is inserted
  int dst_vlabel = vlabel;
  const vector<EdgeMeta> edge_meta = mapping_->get_edge_metas();
  int elabel = 0;
  vector<EdgeMeta>::const_iterator it;
  for (it = edge_meta.begin(); it != edge_meta.end(); ++it, ++elabel) {
    const EdgeMeta &meta = *it;
    if (meta.dst_vlabel != dst_vlabel) {
      continue;
    }
    
    int src_tableid = mapping_->get_table(meta.src_vlabel);
    int dst_vid = offset;
    // XXX: the access of foreign key should be modified
    if (fk == uint64_t(-1))
      fk = *((uint64_t *) (&value[cols[meta.src_fk_col]]));
      uint64_t src_vid = db_.getOffset(src_tableid, fk, ver);  
      string_view edge_data;
      tx.put_edge(src_vid, elabel, dst_vid, edge_data);
      // TODO: no edge property till now
  }

}

}  // namespace graph
}  // namespace nocc

