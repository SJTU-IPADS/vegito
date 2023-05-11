/**
  oendif
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "global_store_ffi.h"
// #include "htap_ds_impl.h"

// using namespace nocc::oltp;
// using namespace nocc::graph;

#include "bench_analytics.h"

#include <cstring>

namespace gaia {

#ifdef __cplusplus
extern "C" {
#endif

// #define NDEBUG true

struct PropertiesIteratorImpl {
  size_t size;
  size_t index;
  VertexId vid;
  AnalyticsCtx* ctx;
  std::vector<BackupStore*> *properties;
  int vlabel_idx;
  int offset;
};

struct GetVertexIteratorImpl {
  VertexId* vids;
  int size;
  size_t index;
  // LabelId* lids;
};

struct GetAllVerticesIteratorImpl {
  int* ranges;
  LabelId* lids;
  int size;
  int index;
  VertexId cur_vertex_id;
};

struct EdgeIteratorImpl {
  Vertex src;
  EpochEdgeIterator* iters;
  int size;
  int index;
  LabelId* dst_vertex_labels;
  LabelId* edge_labels;
  int64_t edge_remaining;
  int64_t current_id;
  int64_t offset;
};

struct GetAllEdgesIteratorImpl {
  LabelId* labels;
  int labels_count;
  AnalyticsCtx* ctx;
  EpochEdgeIterator current_iter;
  int index;
  int current_vertex_id;
  int limit_remaining;
  int* vertex_nums;
};

struct SchemaImpl {
  std::unordered_map<std::string, int> property_id_map;
  std::unordered_map<std::string, int> label_id_map;
  std::unordered_map<std::pair<int, int>, int> dtype_map; // <label_id, property_id> --> dtype
};

union PodProperties {
  bool bool_value;
  char char_value;
  int16_t int16_value;
  int int_value;
  int64_t long_value;
  float float_value;
  double double_value;
};

GraphHandle get_graph_handle(ObjectId object_id, PartitionId channel_num) {
  // FIXME: handle exception here
  // object_id is the pointer of ctx 
  // GraphHandle handle = object_id;
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif

  return reinterpret_cast<void *>(object_id);
}

void free_graph_handle(GraphHandle handle) {
  // TODO 
}

GetVertexIterator get_vertices(GraphHandle graph, PartitionId partition_id,
                               LabelId* labels, VertexId* ids, int count) {

  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__ << "label = " << (ids[0]>>56);
  #endif

  GetVertexIteratorImpl* iter = (GetVertexIteratorImpl*)malloc(sizeof(GetVertexIteratorImpl));  
  // iter->size = count;
  iter->index = 0;
  iter->vids = (VertexId*)malloc(sizeof(VertexId)*count);
  if (labels == NULL) {
    memcpy(iter->vids, ids, sizeof(VertexId)*count);
    iter->size = count;
  } else{
    int cur = 0;
    for (int i = 0; i < count; i++) {
      LabelId lid = (LabelId)((ids[i] >> 56)& 0xff);
      if (lid == labels[i]) {
        iter->vids[cur] = ids[i];
        cur++;
      }
    }
    iter->size = cur;
  }
                               
  return iter;
}

void free_get_vertex_iterator(GetVertexIterator iter) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetVertexIteratorImpl *iter_ptr = reinterpret_cast<GetVertexIteratorImpl *>(iter);
  if (iter != nullptr) {
    if (iter_ptr->vids != NULL) {
      free(iter_ptr->vids);
    }
    free(iter_ptr);
  }
}

int get_vertices_next(GetVertexIterator iter, Vertex* v_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetVertexIteratorImpl* it = (GetVertexIteratorImpl*)iter;
  if(it->index < it->size) {
    //VertexId vid = it->content[it->index];
    //LabelId lid = it->lids[it->index];
    *v_out = it->vids[it->index];
    it->index++;
    return 0;
  } else {
    return -1;
  }
  
}

GetAllVerticesIterator get_all_vertices(GraphHandle graph,
                                        PartitionId partition_id,
                                        LabelId* labels, int labels_count,
                                        int64_t limit) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetAllVerticesIteratorImpl* ret = (GetAllVerticesIteratorImpl*)malloc(sizeof(GetAllVerticesIteratorImpl));
  ret->index = 0;
  ret->cur_vertex_id = 0;
  ret->size = 0;
  if (limit == 0) {
    ret->size = 0;
    ret->ranges = NULL;
    ret->lids = NULL;
  }
  else if (labels_count == 0 || labels == NULL) {
    AnalyticsCtx* ctx= (AnalyticsCtx*)(graph);
    labels_count = (int)(ctx->vertex_labels.size());
    ret->ranges = (int*)malloc(sizeof(int) * labels_count);
    ret->lids = (LabelId*)malloc(sizeof(LabelId) * labels_count);
    size_t limit_remaining = limit;
    for (int i = 0; i < labels_count; i++) {
      if (ctx->vertex_nums[i] <= limit_remaining) {
        ret->ranges[i] = ctx->vertex_nums[i];
        ret->lids[i] = ctx->vertex_labels[i];
        limit_remaining -= ctx->vertex_nums[i];
        ret->size += 1;
      }
      else {
        ret->ranges[i] = limit_remaining;
        ret->lids[i] = ctx->vertex_labels[i];
        ret->size += 1;
        break;
      }
    }
    // ret->size = labels_count;
  }
  else{
    AnalyticsCtx* ctx= (AnalyticsCtx*)(graph);
    ret->ranges = (int*)malloc(sizeof(int) * labels_count);
    ret->lids = (LabelId*)malloc(sizeof(LabelId) * labels_count);
    size_t limit_remaining = limit;
    for (int i = 0; i < labels_count; i++) {
      int idx = ctx->vlabel2idx[labels[i]];
      if (ctx->vertex_nums[idx] <= limit_remaining) {
        ret->ranges[i] = ctx->vertex_nums[idx];
        ret->lids[i] = ctx->vertex_labels[idx];
        limit_remaining -= ctx->vertex_nums[idx];
        ret->size += 1;
      }
      else {
        ret->ranges[i] = limit_remaining;
        ret->lids[i] = ctx->vertex_labels[idx];
        ret->size += 1;
        break;
      }
    }
    // ret->size = labels_count;
  }
  return ret;
}

void free_get_all_vertices_iterator(GetAllVerticesIterator iter) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetAllVerticesIteratorImpl *iter_ptr = reinterpret_cast<GetAllVerticesIteratorImpl *>(iter);
  if (iter != nullptr) {
    if (iter_ptr->ranges != NULL) {
      free(iter_ptr->ranges);
    }
    if (iter_ptr->lids != NULL) {
      free(iter_ptr->lids);
    }
    free(iter_ptr);
  }
  
}

int get_all_vertices_next(GetAllVerticesIterator iter, Vertex* v_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetAllVerticesIteratorImpl* it = (GetAllVerticesIteratorImpl*)iter;
  if (it->index < it->size) {
    if (it->cur_vertex_id < it->ranges[it->index]) {
      VertexId vid = it->cur_vertex_id;
      LabelId lid = it->lids[it->index];
      *v_out = vid | (((VertexId)lid)<<56);
      it->cur_vertex_id++;
      return 0;
    }
    else if (it->index != it->size -1) {
      it->cur_vertex_id = 0;
      it->index++;
      VertexId vid = 0;
      LabelId lid = it->lids[it->index];
      *v_out = vid | (((VertexId)lid)<<56);
      it->cur_vertex_id++;
      return 0;
    }
    else {
      return -1;
    }

  }
  else {
    return -1;
  }
  
}

VertexId get_vertex_id(GraphHandle graph, Vertex v) { 
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return (VertexId)(v); 
  }

OuterId get_outer_id(GraphHandle graph, Vertex v) {
  // FIXME
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return (VertexId)v;
}

int get_vertex_by_outer_id(GraphHandle graph, LabelId label_id,
                           OuterId outer_id, Vertex* v) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
}

OuterId get_outer_id_by_vertex_id(GraphHandle graph, VertexId v) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
}

LabelId get_vertex_label(GraphHandle graph, Vertex v) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return (LabelId)((v >> 56)& 0xff);
}

int get_vertex_property(GraphHandle handle, Vertex v, PropertyId id,
                        Property* p_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__ << " label = " << (LabelId)((v >> 56)& 0xff);
  #endif
  LabelId lid = (LabelId)((v >> 56)& 0xff);
  VertexId vid = (VertexId)(v & 0x0ffffffffffffff);
  AnalyticsCtx* ctx = (AnalyticsCtx*)(handle);
  std::vector<BackupStore*> &propertys = ctx->propertys;
  int vlabel_idx = ctx->vlabel2idx[lid];
  // p_out->len = propertys[vlabel_idx]->get_val_lens()[id];
  // auto dtype = static_cast<PropertyType>(propertys[vlabel_idx]->get_val_types()[id]);
  // p_out->data = (void*) propertys[vlabel_idx]->getByOffset(vid, id, ctx->read_epoch_id);
  auto offset = ctx->graph_store->get_schema().vlabel2prop_offset[lid];
  auto dtype = static_cast<PropertyType>(propertys[vlabel_idx]->get_val_types()[id - offset]);
  // LOG(INFO) << " lid = " << lid << " offset = " << offset << " PropertyId = " << id << " vlabel_idx = " << vlabel_idx; 
  auto data = (char*) propertys[vlabel_idx]->getByOffset(vid, id - offset, ctx->read_epoch_id);
  if ((dtype >= BOOL) && (dtype <= DOUBLE)) {
    if (dtype == BOOL) {
      p_out->type = BOOL;
      p_out->len = *((bool*)(data));
    } else if (dtype == CHAR) {
      p_out->type = CHAR;
      p_out->len = *((char*)(data));
    } else if (dtype == INT) {
      p_out->type = INT;
      p_out->len = *((int*)(data));
    } else if (dtype == FLOAT) {
      p_out->type = FLOAT;
      p_out->len = *((float*)(data));
    } else if (dtype == DOUBLE) {
      p_out->type = DOUBLE;
      p_out->len = *((double*)(data));
    } else if (dtype == LONG) {
      p_out->type = LONG;
      p_out->len = *((int64_t*)(data));
    }
  }
  else if (dtype == STRING) {
    p_out->type = STRING;
    auto tmp = (inline_str_8<40U>*)data;
    p_out->len = tmp->size();
    p_out->data = const_cast<char*>(tmp->c_str());
    /*
    p_out->len = 0;
    while (data[p_out->len] != '\0') {
      p_out->len += 1;
    }
    p_out->data = data;
    */
    //strncpy(p_out->data, data, p_out->len);
    //memcpy(p_out->data, data, p_out->len*sizeof(char));
  }
  else if (dtype == DATE) {
    p_out->type = STRING;
    p_out->len = 10;
    p_out->data = const_cast<char*>(((inline_str_fixed<10>*)data)->data());
  }        
  else if (dtype == DATETIME) {
    p_out->type = STRING;
    p_out->len = 28;
    p_out->data = const_cast<char*>(((inline_str_fixed<28>*)data)->data());
  }
  else if (dtype == TEXT) {
    p_out->type = STRING;
    p_out->len = ((inline_str_16<2000>*)data)->size();
    p_out->data = const_cast<char*>(((inline_str_16<2000>*)data)->c_str());
  }

  /*
  if ((p_out->type >= 1) && (p_out->type <= 7)) {// POD type
      PodProperties pp;
      if (p_out->type == 7) {
        // double
        pp.double_value = *((double*)(p_out->data));
      }
      else if (p_out->type == 2) {
        // char
        pp.char_value = *((char*)(p_out->data));
      }
      else {
        pp.long_value = *((int64_t*)(p_out->data));
      }
  } */
  else {
    // store string
  }
  p_out->id = id;

  return 0;
}

PropertiesIterator get_vertex_properties(GraphHandle graph, Vertex v) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  LabelId lid = (LabelId)((v >> 56)& 0xff);
  VertexId vid = (VertexId)(v & 0x0ffffffffffffff);
  AnalyticsCtx* ctx = (AnalyticsCtx*)(graph);
  int vlabel_idx = ctx->vlabel2idx[lid];

  PropertiesIteratorImpl* ret = (PropertiesIteratorImpl *)malloc(sizeof(PropertiesIteratorImpl));
  ret->size = ctx->propertys[vlabel_idx]->get_val_lens().size();
  ret->index = 0;
  ret->vid = vid;
  ret->ctx = ctx;
  ret->properties = &(ctx->propertys);
  ret->vlabel_idx = vlabel_idx;
  ret->offset = ctx->graph_store->get_schema().vlabel2prop_offset[lid];
  /*
  ret->offset = 0;
  for (int i = 0; i < vlabel_idx; i++) {
    ret->offset += ctx->propertys[i]->get_val_lens().size(); 
  }
  */
  return ret;
}

OutEdgeIterator get_out_edges(GraphHandle graph, PartitionId partition_id,
                              VertexId src_id, LabelId* labels,
                              int labels_count, int64_t limit) {
  // EdgeIteratorImpl* iter = (EdgeIteratorImpl*)malloc(sizeof(EdgeIteratorImpl));
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__ << " elabel = " << labels[0] << " vlabel = " << (src_id >> 56);
  #endif
  EdgeIteratorImpl* iter = new EdgeIteratorImpl();
  iter->index = 0;
  iter->size = 0;
  iter->src = src_id;
  iter->edge_remaining = 0;
  iter->current_id = 0;
  iter->offset = 0;
  AnalyticsCtx* ctx = (AnalyticsCtx*)(graph);
  VertexId vid = src_id& 0x0ffffffffffffff;
  LabelId lid = (LabelId)((src_id >> 56)& 0xff);
  int vlabel_idx = ctx->vlabel2idx[lid];
  auto reader = ctx->seg_graph_readers[vlabel_idx];
  int64_t limit_remaining = limit;
  if (labels == NULL || labels_count == 0) {
    labels_count = (int)ctx->edge_labels.size();
    //iter->iters = (EpochEdgeIterator*)malloc(sizeof(EpochEdgeIterator)*labels_count);
    iter->iters = new EpochEdgeIterator[labels_count];
    iter->dst_vertex_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    iter->edge_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    for (int i = 0; i < labels_count; i++) {
      LabelId edge_label = ctx->edge_labels[i];
      int elabel_idx = ctx->elabel2idx[edge_label];
      // elabel_idx = ctx->graph_store->get_schema().elabel_in_out_map[elabel_idx].first;
      auto edge_iter = reader->get_edges(vid, edge_label, EOUT);
      auto edge_size = edge_iter.size();
      if (edge_size != 0) {
        iter->iters[iter->size] = reader->get_edges(vid, edge_label, EOUT);
        auto dst_vlabel_idx = ctx->dst_vlabel_idx[elabel_idx];
        iter->dst_vertex_labels[iter->size] = ctx->vertex_labels[dst_vlabel_idx];
        // iter->dst_vertex_labels[iter->size] = ctx->dst_vlabel_idx[elabel_idx];
        iter->edge_labels[iter->size] = edge_label;
        iter->size++;
        if (edge_size <= limit_remaining) {
          limit_remaining -= edge_size;
          iter->edge_remaining = edge_size;
        }
        else {
           iter->edge_remaining = limit_remaining;
          break;
        }
      }
    }
  }
  else {
    //iter->iters = (EpochEdgeIterator*)malloc(sizeof(EpochEdgeIterator)*labels_count);
    iter->iters = new EpochEdgeIterator[labels_count];
    iter->dst_vertex_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    iter->edge_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    for (int i = 0; i < labels_count; i++) {
      LabelId edge_label = labels[i];
      // edge_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].first;
      int elabel_idx = ctx->elabel2idx[edge_label];
      auto edge_iter = reader->get_edges(vid, edge_label, EOUT);
      auto edge_size = edge_iter.size();
      if (edge_size != 0) {
        iter->iters[iter->size] = reader->get_edges(vid, edge_label, EOUT);
        auto dst_vlabel_idx = ctx->dst_vlabel_idx[elabel_idx];
        iter->dst_vertex_labels[iter->size] = ctx->vertex_labels[dst_vlabel_idx];
        // iter->dst_vertex_labels[iter->size] = ctx->dst_vlabel_idx[elabel_idx];
        iter->edge_labels[iter->size] = edge_label;
        iter->size++;
        if (edge_size <= limit_remaining) {
          limit_remaining -= edge_size;
          iter->edge_remaining = edge_size;
        }
        else {
           iter->edge_remaining = limit_remaining;
          break;
        }
      }
    }    
  }

  return iter;
}

// 释放迭代器
void free_out_edge_iterator(OutEdgeIterator iter) {
  EdgeIteratorImpl *iter_ptr = reinterpret_cast<EdgeIteratorImpl *>(iter);
  if (iter != nullptr) {
    if (iter_ptr->iters != NULL) {
      //free(iter_ptr->iters);
      delete [] iter_ptr->iters;
    }
    if (iter_ptr->dst_vertex_labels != NULL) {
      free(iter_ptr->dst_vertex_labels);
    }
    if (iter_ptr->edge_labels != NULL) {
      free(iter_ptr->edge_labels);
    }
    free(iter_ptr);
  }
}

int out_edge_next(OutEdgeIterator iter, struct Edge* e_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  EdgeIteratorImpl* it = (EdgeIteratorImpl*)iter;

  // no edge
  if(it->size == 0) return -1;

  if (it->iters[it->index].valid() && it->index < it->size - 1) {
    e_out->src = it->src;
    e_out->dst = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
    VertexId tmp = (it->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
    e_out->offset = tmp | (it->iters[it->index].dst_id() & 0x07efffffff);
    it->iters[it->index].next();
    it->offset += 1;
    return 0;
  } 
  else if (it->iters[it->index].valid() && it->index == it->size - 1) {
    if (it->current_id < it->edge_remaining) {
      e_out->src = it->src;
      e_out->dst = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
      VertexId tmp = (it->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
      e_out->offset = tmp | (it->iters[it->index].dst_id() & 0x07efffffff);
      it->iters[it->index].next();
      it->offset += 1;
      it->current_id += 1;
      return 0;
    }
    else {
      return -1;
    }
  }
  else if (it->index != it->size -1) {
    it->index++;
    it->offset = 0;
    e_out->src = it->src;
    e_out->dst = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
    VertexId tmp = (it->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
    e_out->offset = tmp | (it->iters[it->index].dst_id() & 0x07efffffff);
    if (it->index == it->size -1) {
      it->current_id += 1;
    }
    it->iters[it->index].next();
    it->offset += 1;
    return 0;
  }
  return -1;

}

InEdgeIterator get_in_edges(GraphHandle graph, PartitionId partition_id,
                            VertexId dst_id, LabelId* labels, int labels_count,
                            int64_t limit) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  EdgeIteratorImpl* iter = new EdgeIteratorImpl();
  iter->index = 0;
  iter->size = 0;
  iter->src = dst_id;
  iter->edge_remaining = 0;
  iter->current_id = 0;
  iter->offset = 0;
  AnalyticsCtx* ctx = (AnalyticsCtx*)(graph);
  VertexId vid = dst_id& 0x0ffffffffffffff;
  LabelId lid = (LabelId)((dst_id >> 56)& 0xff);
  int vlabel_idx = ctx->vlabel2idx[lid];
  auto reader = ctx->seg_graph_readers[vlabel_idx];
  int64_t limit_remaining = limit;
  if (labels == NULL || labels_count == 0) {
    labels_count = (int)ctx->edge_labels.size();
    //iter->iters = (EpochEdgeIterator*)malloc(sizeof(EpochEdgeIterator)*labels_count);
    iter->iters = new EpochEdgeIterator[labels_count];
    iter->dst_vertex_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    iter->edge_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    for (int i = 0; i < labels_count; i++) {
      LabelId edge_label = ctx->edge_labels[i];
      int elabel_idx = ctx->elabel2idx[edge_label];
      // elabel_idx = ctx->graph_store->get_schema().elabel_in_out_map[elabel_idx].first;
      // auto out_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].first;
      // int elabel_idx = ctx->elabel2idx[out_label];
      // auto in_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].second;
      auto edge_iter = reader->get_edges(vid, edge_label, EIN);
      auto edge_size = edge_iter.size();
      // elabel_idx = ctx->graph_store->get_schema().elabel_in_out_map[elabel_idx].first;

      if (edge_size!=0) {
        iter->iters[iter->size] = reader->get_edges(vid, edge_label, EIN);
        auto dst_vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
        iter->dst_vertex_labels[iter->size] = ctx->vertex_labels[dst_vlabel_idx];
        // iter->dst_vertex_labels[iter->size] = ctx->src_vlabel_idx[elabel_idx];
        iter->edge_labels[iter->size] = edge_label;
        iter->size++;
        if (edge_size <= limit_remaining) {
          limit_remaining -= edge_size;
          iter->edge_remaining = edge_size;
        }
        else {
           iter->edge_remaining = limit_remaining;
          break;
        }
      }
    }
  }
  else {
    //iter->iters = (EpochEdgeIterator*)malloc(sizeof(EpochEdgeIterator)*labels_count);
    iter->iters = new EpochEdgeIterator[labels_count];
    iter->dst_vertex_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    iter->edge_labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
    for (int i = 0; i < labels_count; i++) {
      LabelId edge_label = labels[i];
      int elabel_idx = ctx->elabel2idx[edge_label];
      // auto out_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].first;
      // int elabel_idx = ctx->elabel2idx[out_label];
      // auto in_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].second;
      // LOG(INFO) << "in label = " << in_label << " vid = " << vid;
      auto edge_iter = reader->get_edges(vid, edge_label, EIN);
      auto edge_size = edge_iter.size();
      // LOG(INFO) << "neighbor size = " << edge_size;
      if (edge_size != 0) {
        iter->iters[iter->size] = reader->get_edges(vid, edge_label, EIN);
        auto dst_vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
        iter->dst_vertex_labels[iter->size] = ctx->vertex_labels[dst_vlabel_idx];
        // iter->dst_vertex_labels[iter->size] = ctx->src_vlabel_idx[elabel_idx];
        iter->edge_labels[iter->size] = edge_label;
        iter->size++;
        if (edge_size <= limit_remaining) {
          limit_remaining -= edge_size;
          iter->edge_remaining = edge_size;
        }
        else {
           iter->edge_remaining = limit_remaining;
          break;
        }
      }
    }    
  }

  return iter;                            
}

void free_in_edge_iterator(InEdgeIterator iter) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  EdgeIteratorImpl *iter_ptr = reinterpret_cast<EdgeIteratorImpl *>(iter);
  if (iter != nullptr) {
    if (iter_ptr->iters != NULL) {
      //free(iter_ptr->iters);
      delete [] iter_ptr->iters;
    }
    if (iter_ptr->dst_vertex_labels != NULL) {
      free(iter_ptr->dst_vertex_labels);
    }
    if (iter_ptr->edge_labels != NULL) {
      free(iter_ptr->edge_labels);
    }
    free(iter_ptr);
  }
}

int in_edge_next(InEdgeIterator iter, struct Edge* e_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  EdgeIteratorImpl* it = (EdgeIteratorImpl*)iter;

  // no edge
  if(it->size == 0) return -1;

  if (it->iters[it->index].valid() && it->index < it->size - 1) {
    e_out->dst = it->src;
    e_out->src = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
    VertexId tmp = (e_out->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
    e_out->offset = tmp | (e_out->dst & 0x07efffffff);
    it->iters[it->index].next();
    return 0;
  } 
  else if (it->iters[it->index].valid() && it->index == it->size - 1) {
    if (it->current_id < it->edge_remaining) {
      e_out->dst = it->src;
      e_out->src = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
      VertexId tmp = (e_out->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
      e_out->offset = tmp | (e_out->dst & 0x07efffffff);
      it->iters[it->index].next();
      it->current_id += 1;
      return 0;
    }
    else {
      return -1;
    }
  }
  else if (it->index != it->size -1) {
    it->index++;
    e_out->dst = it->src;
    e_out->src = it->iters[it->index].dst_id() | (((VertexId)it->dst_vertex_labels[it->index])<<56);
    VertexId tmp = (e_out->src << 35) | ((VertexId)(it->edge_labels[it->index]))<<29;
    e_out->offset = tmp | (e_out->dst & 0x07efffffff);
    if (it->index == it->size -1) {
      it->current_id += 1;
    }
    it->iters[it->index].next();
    return 0;
  }
  return -1;
}

GetAllEdgesIterator get_all_edges(GraphHandle graph, PartitionId partition_id,
                                  LabelId* labels, int labels_count,
                                  int64_t limit) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetAllEdgesIteratorImpl* iter = new GetAllEdgesIteratorImpl();
  iter->index = 0;
  iter->current_vertex_id = 0;
  iter->labels_count = labels_count;
  iter->limit_remaining = limit;
  AnalyticsCtx* ctx = (AnalyticsCtx*)(graph);
  iter->ctx = ctx;
  iter->labels = (LabelId*)malloc(sizeof(LabelId)*labels_count);
  memcpy(iter->labels, labels, sizeof(LabelId)*labels_count);
  // iter->current_iter = new EpochEdgeIterator();
  LabelId edge_label = labels[0];
  // edge_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].first;
  int elabel_idx = ctx->elabel2idx[edge_label];
  int vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
  auto reader = ctx->seg_graph_readers[vlabel_idx];
  iter->current_iter = reader->get_edges(0, edge_label);
  iter->vertex_nums = (int*)malloc(sizeof(int)*labels_count);
  for (int i = 0; i < labels_count; i++) {
    LabelId edge_label = labels[i];
    // edge_label = ctx->graph_store->get_schema().elabel_in_out_map[edge_label].first;
    int elabel_idx = ctx->elabel2idx[edge_label];
    int vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
    iter->vertex_nums[i] = ctx->vertex_nums[vlabel_idx];
  }

  return iter;
}

// 释放迭代器
void free_get_all_edges_iterator(GetAllEdgesIterator iter) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  GetAllEdgesIteratorImpl *iter_ptr = reinterpret_cast<GetAllEdgesIteratorImpl *>(iter);
  if (iter != nullptr) {
    if (iter_ptr->labels != NULL) {
      free(iter_ptr->labels);
    }
    if (iter_ptr->vertex_nums != NULL) {
      free(iter_ptr->vertex_nums);
    }
    free(iter_ptr);
  }
}

int get_all_edges_next(GetAllEdgesIterator iter, struct Edge* e_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  if (!iter) {
    return -1;
  }

  GetAllEdgesIteratorImpl* iter_ptr = (GetAllEdgesIteratorImpl*)iter;
  if (iter_ptr->labels_count == 0) {
    return -1;
  }
  if (iter_ptr->limit_remaining <=0) {
    return -1;
  }
  auto ctx = iter_ptr->ctx;
  if (iter_ptr->current_iter.valid()) {
    int elabel = iter_ptr->labels[iter_ptr->index];
    // elabel = ctx->graph_store->get_schema().elabel_in_out_map[elabel].first;
    int elabel_idx = ctx->elabel2idx[elabel];
    int src_vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
    LabelId src_label = ctx->vertex_labels[src_vlabel_idx];
    e_out->src = iter_ptr->current_vertex_id | (((VertexId)src_label)<<56);

    int dst_vlabel_idx = ctx->dst_vlabel_idx[elabel_idx];
    LabelId dst_label = ctx->vertex_labels[dst_vlabel_idx];
    e_out->dst = iter_ptr->current_iter.dst_id() |  (((VertexId)dst_label)<<56);

    VertexId tmp = (e_out->src << 35) | ((VertexId)(elabel))<<29;
    e_out->offset = tmp | (e_out->dst & 0x07efffffff);

    iter_ptr->current_iter.next();

    return 0;

  }
  else if (iter_ptr->current_vertex_id < iter_ptr->vertex_nums[iter_ptr->index]) {
    iter_ptr->current_vertex_id += 1;
    int elabel = iter_ptr->labels[iter_ptr->index];
    // elabel = ctx->graph_store->get_schema().elabel_in_out_map[elabel].first;
    int elabel_idx = ctx->elabel2idx[elabel];
    int src_vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
    iter_ptr->current_iter = ctx->seg_graph_readers[src_vlabel_idx]->get_edges(iter_ptr->current_vertex_id, elabel);
    while (iter_ptr->current_iter.size()==0 && iter_ptr->current_vertex_id < iter_ptr->vertex_nums[iter_ptr->index]) {
      iter_ptr->current_vertex_id += 1;
      iter_ptr->current_iter = ctx->seg_graph_readers[src_vlabel_idx]->get_edges(iter_ptr->current_vertex_id, elabel);
      if (iter_ptr->current_iter.size() > 0) {
        break;
      }
    }

    if (iter_ptr->current_iter.size() != 0) {
      int dst_vlabel_idx = ctx->dst_vlabel_idx[elabel_idx];
      LabelId dst_label = ctx->vertex_labels[dst_vlabel_idx];
      e_out->dst = iter_ptr->current_iter.dst_id() |  (((VertexId)dst_label)<<56);

      VertexId tmp = (e_out->src << 35) | ((VertexId)(elabel))<<29;
      e_out->offset = tmp | (e_out->dst & 0x07efffffff);

      iter_ptr->current_iter.next();
      return 0;
    }
  }
  else if (iter_ptr->index < iter_ptr->labels_count -1) {
    iter_ptr->index += 1;
    iter_ptr->current_vertex_id = 0;

    int elabel = iter_ptr->labels[iter_ptr->index];
    // elabel = ctx->graph_store->get_schema().elabel_in_out_map[elabel].first;
    int elabel_idx = ctx->elabel2idx[elabel];
    int src_vlabel_idx = ctx->src_vlabel_idx[elabel_idx];
    iter_ptr->current_iter = ctx->seg_graph_readers[src_vlabel_idx]->get_edges(iter_ptr->current_vertex_id, elabel);
    while (iter_ptr->current_iter.size()==0 && iter_ptr->current_vertex_id < iter_ptr->vertex_nums[iter_ptr->index]) {
      iter_ptr->current_vertex_id += 1;
      iter_ptr->current_iter = ctx->seg_graph_readers[src_vlabel_idx]->get_edges(iter_ptr->current_vertex_id, elabel);
      if (iter_ptr->current_iter.size() > 0) {
        break;
      }
    }

    
    int dst_vlabel_idx = ctx->dst_vlabel_idx[elabel_idx];
    LabelId dst_label = ctx->vertex_labels[dst_vlabel_idx];
    e_out->dst = iter_ptr->current_iter.dst_id() |  (((VertexId)dst_label)<<56);

    VertexId tmp = (e_out->src << 35) | ((VertexId)(elabel))<<29;
    e_out->offset = tmp | (e_out->dst & 0x07efffffff);

    iter_ptr->current_iter.next();
    return 0;
   
  }
  else {
    return -1;
  }

}

VertexId get_edge_src_id(GraphHandle graph, struct Edge* e) { 
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return e->src;
  }

VertexId get_edge_dst_id(GraphHandle graph, struct Edge* e) { 
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return e->dst; 
  }

static void parse_edge_id(GraphHandle graph, EdgeId eid,
                          PartitionId* fid, LabelId* label,
                          int64_t* offset) {
  // LOG(INFO)<<"parse_edge_id ...";
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return;
}

EdgeId get_edge_id(GraphHandle graph, struct Edge* e) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return e->offset;
}

LabelId get_edge_src_label(GraphHandle graph, struct Edge* e) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__ << " label id = " << (LabelId)((e->src >> 56)&0xff);
  #endif
  return (LabelId)((e->src >> 56)&0xff);
}

LabelId get_edge_dst_label(GraphHandle graph, struct Edge* e) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__ << " label id = " << (LabelId)((e->dst >> 56)&0xff);
  #endif
  return (LabelId)((e->dst >> 56)&0xff);
}

LabelId get_edge_label(GraphHandle graph, struct Edge* e) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
}

int get_edge_property(GraphHandle graph, struct Edge* e, PropertyId id,
                      Property* p_out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
   p_out->type = INT;
   p_out->len = 10;
   return 0;
}

PropertiesIterator get_edge_properties(GraphHandle graph, struct Edge* e) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  PropertiesIteratorImpl* ret = (PropertiesIteratorImpl *)malloc(sizeof(PropertiesIteratorImpl));
  ret->index = 0;
  ret->size = 0;
  return ret;
}

int properties_next(PropertiesIterator iter, Property* p_out) {
  //#ifndef NDEBUG
  //  LOG(INFO) << "enter " << __FUNCTION__;
  //#endif
  if (!iter) {
    return -1;
  }
  PropertiesIteratorImpl* it = (PropertiesIteratorImpl*)iter;
  //  LOG(INFO) << "enter " << __FUNCTION__ << " VID = " << it->vid << " Idx  = " << it->index << " Size = " <<it->size;
  if (it->index < it->size) {
    p_out->id = (int)(it->index) + it->offset;
    char* data = (*(it->properties))[it->vlabel_idx]->getByOffset(
      it->vid, it->index, it->ctx->read_epoch_id);
    auto &dtypes = (*(it->properties))[it->vlabel_idx]->get_val_types();
    if (data == nullptr) {
      LOG(INFO) << "ERROR: data is nullptr: dtypes[it->index] = " << dtypes[it->index]
                << ", vid = " << it->vid
                << ", property id = " << it->index
                << ", label id = " << it->vlabel_idx;
      throw std::runtime_error("...... ..... .....");
    }
    switch (dtypes[it->index]) {
      case BOOL:
        p_out->type = BOOL;
        p_out->len = *((bool *)data);
      case CHAR:
        p_out->type = CHAR;
        p_out->len = *((char *)data);
        break;
      case INT:
        p_out->type = INT;
        p_out->len = *((int32_t *)data);
        break;
      case LONG:
        p_out->type = LONG;
        p_out->len = *((int64_t *)data);
        break;
      case FLOAT:
        p_out->type = FLOAT;
        p_out->len = *((float *)data);
        break;
      case DOUBLE:
        p_out->type = DOUBLE;
        p_out->len = *((double *)data);
        break;
      case STRING: {
        p_out->type = STRING;
        auto tmp = (inline_str_8<40U>*)data;
        p_out->len = tmp->size();
        p_out->data = const_cast<char*>(tmp->c_str());
        /*
        p_out->len = 0;
        while (data[p_out->len] != '\0') {
          p_out->len += 1;
        }
        p_out->data = data;
        */
        //strncpy(p_out->data, data, p_out->len);
        //memcpy(p_out->data, data, p_out->len*sizeof(char));
        break;
      }
      case DATE:
        p_out->type = STRING;
        p_out->len = 10;
        p_out->data = const_cast<char*>(((inline_str_fixed<10>*)data)->data());
        break;
      case DATETIME:
        p_out->type = STRING;
        p_out->len = 28;
        p_out->data = const_cast<char*>(((inline_str_fixed<28>*)data)->data());
        break;
      case TEXT:
        p_out->type = STRING;
        p_out->len = ((inline_str_16<2000>*)data)->size();
        p_out->data = const_cast<char*>(((inline_str_16<2000>*)data)->c_str());
        break;
      case BYTES:
        assert(false);
      default:
        assert(false);
    }
    it->index += 1;
    return 0;
  } else {
    return -1;
  }
}

void free_properties_iterator(PropertiesIterator iter) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  PropertiesIteratorImpl *iter_ptr = reinterpret_cast<PropertiesIteratorImpl *>(iter);
  if (iter != nullptr) {
    free(iter_ptr);
  }
}

int get_property_as_bool(Property* property, bool* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (bool)property->len;
  return 0;
}
int get_property_as_char(Property* property, char* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (char)property->len;
  return 0;
}
int get_property_as_short(Property* property, int16_t* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (int16_t)property->len;
  return 0;
}
int get_property_as_int(Property* property, int* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (int)property->len;
  return 0;
}
int get_property_as_long(Property* property, int64_t* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (int64_t)property->len;
  return 0;
}
int get_property_as_float(Property* property, float* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (float)property->len;
  return 0;
}
int get_property_as_double(Property* property, double* out) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  *out = (double)property->len;
  return 0;
}

int get_property_as_string(Property* property, const char** out, int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  //int r = htap_impl::get_property_as_string(property, out, out_len);
  *out_len = property->len;
  *out = static_cast<char*>(property->data);
  return 0;
}
int get_property_as_bytes(Property* property, const char** out, int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return 0;
}
int get_property_as_int_list(Property* property, const int** out,
                             int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return 0;
}
int get_property_as_long_list(Property* property, const int64_t** out,
                              int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return 0;
}
int get_property_as_float_list(Property* property, const float** out,
                               int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif                               
  return 0;
}
int get_property_as_double_list(Property* property, const double** out,
                                int* out_len) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif
  return 0;
}
int get_property_as_string_list(Property* property, const char*** out,
                                const int** out_len, int* out_num) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif                                
  return 0;
}

void free_property(Property* property) {}

// *out_num为string的个数
// (*out_len)[i]为第i个string的长度
// (*out)[i]为第i个string的其实地址

PartitionId get_partition_id(GraphHandle graph, VertexId v) {
  return 0;
}

// 如果 key 不存在，返回 -1
// 否则返回0，结果存在 internal_id 和 partition_id 中
int get_vertex_id_from_primary_key(GraphHandle graph, LabelId label_id,
                                   const char* key, VertexId* internal_id,
                                   PartitionId* partition_id) {
  #ifndef NDEBUG
    LOG(INFO) << "enter " << __FUNCTION__;
  #endif                                   
  AnalyticsCtx* ctx = (AnalyticsCtx*)(graph);
  uint64_t pri_key = strtoull(key, NULL, 0);
  // TODO: handle primary key not exist
  VertexId vid = ctx->rg_map->get_key2vid(label_id, pri_key);
  *internal_id = vid | (((VertexId)label_id)<<56);
  *partition_id = 0;
  // currently we always return 0
  return 0;
}

void get_process_partition_list(GraphHandle graph, PartitionId** partition_ids,
                                int* partition_id_size) {

}

void free_partition_list(PartitionId* partition_ids) { free(partition_ids); }

#ifdef __cplusplus
}
#endif

}
