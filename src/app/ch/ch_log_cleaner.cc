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

#include "ch_log_cleaner.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"

#include "framework/framework_cfg.h"
#include "framework/view_manager.h"

#include <stdexcept>
#include <vector>

using namespace std;
using namespace nocc::framework;

namespace {
  // graph
  thread_local uint64_t last_o_id;
  thread_local uint64_t *last_o_edge;
}

namespace nocc {
namespace oltp {
namespace ch {

ChLogCleaner::ChLogCleaner()
    : LogCleaner(true) {
  // for (uint w = 0; w < NumWarehouses(); ++w) {
  //   for (uint d = 0; d < NumDistrictsPerWarehouse(); ++d) {
  //     // 3000 is magic number, which is the init num of new order
  //     last_no_o_ids_[w][d] = 3000;
  //   }
  // }

  ware_tp_col_ = { W_YTD };
  dist_tn_col_ = { D_NEXT_O_ID };
  dist_tp_col_ = { D_YTD };
  cust_tp_col_ = { C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA };
  cust_td_col_ = { C_BALANCE, C_DELIVERY_CNT };
  orde_td_col_ = { O_CARRIER_ID };
  orli_td_col_ = { OL_DELIVERY_D };
  const int M = config.getColSplitType();  // always M == 2 
  if (M != 2) {
    stoc_tn_col_ = { S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT };
  } else {
#if 0
    stoc_tn_col_ = { S_QUANTITY, /* S_YTD,*/ S_ORDER_CNT, S_REMOTE_CNT };
#else
    stoc_tn_col_ = { S_QUANTITY };
#endif
  }
}

int ChLogCleaner::clean_log(int table_id, uint64_t key, uint64_t seq,
                            char *val,int length) {
  assert(false);
}

int ChLogCleaner::clean_log(int log_id, int partition_id, int tx_id,
                            int table_id, uint64_t key, uint64_t seq, char *val,
                            int length, uint32_t op_bit, uint64_t write_epoch) {
  BackupDB *db  = backup_map_[partition_id];

  // TODO: for single machine
  if (config.getNumServers() == 1) { 
    assert(backup_map2_[partition_id]);
    if (log_id == 1 && backup_map2_[partition_id]) {
      db = backup_map2_[partition_id];
    }
  }

  assert(db);
  if (op_bit == 0) {
    printf("tx_id %d table %d\n", tx_id, table_id);
    assert(false);
  }

  if (op_bit == LOG_OP_I) {
    assert(seq == 2);
    // TODO: HIST is unused and w/o primary key
    if (table_id == HIST) return 0;

    uint64_t row_id = db->Insert(table_id, key, val, write_epoch);

    // OL_GRAPH
    if (table_id == ORDE) {
      last_o_id = key;
      last_o_edge = db->getEdge(table_id, row_id);
    }
    if (table_id == ORLI) {
      assert(last_o_id == orderLineKeyToOrderKey(key));
      uint64_t &num = last_o_edge[0];
      last_o_edge[num] = row_id;
      ++num;
    }
    return 0;
  }

  if (unlikely(op_bit == LOG_OP_D)) {
    assert(tx_id == TID_DELIVERY && table_id == NEWO && length == 0);
    int32_t no_o_id = static_cast<int32_t>(key << 32 >> 32);
    int32_t upper   = newOrderUpper(key);

    int32_t d_id = upper % 10;
    int32_t w_id = upper / 10;
    if(d_id == 0) {
      w_id -= 1;
      d_id = 10;
    }
    // hard coded here to delete the new order entry
    // TODO: 240 is so small
    // last_no_o_ids_[w_id - 1][d_id - 1] = no_o_id + 1;
    return 0;
  }

  // update cases
  assert(seq > 2 && seq % 2 == 0);

  switch(table_id) {
    case WARE: 
      assert(tx_id == TID_PAYMENT);
      db->Update(table_id, key, val, ware_tp_col_, seq, write_epoch);
      return 0;  
    case DIST: {
      const auto &col = (tx_id == TID_NEW_ORDER)? dist_tn_col_ : dist_tp_col_;
      db->Update(table_id, key, val, col, seq, write_epoch);
      return 0;
    }  
    case CUST: {
      const auto &col = (tx_id == TID_PAYMENT)? cust_tp_col_ : cust_td_col_;
      db->Update(table_id, key, val, col, seq, write_epoch);
      return 0; 
    } 
    case ORDE: {
      assert(tx_id == TID_DELIVERY);
      db->Update(table_id, key, val, orde_td_col_, seq, write_epoch);
      return 0;
    }
    case ORLI: {
      assert(tx_id == TID_DELIVERY);
      vector<int> cols { OL_DELIVERY_D };
      db->Update(table_id, key, val, orli_td_col_, seq, write_epoch);
      return 0;
    }
    case STOC: {
      assert(tx_id == TID_NEW_ORDER); 
      db->Update(table_id, key, val, stoc_tn_col_, seq, write_epoch);
      return 0;
    }
    default:
      assert(false);
  }

  assert(false);
  return 0;

#if 0
  int w_id;

  // for micro benchmark
  // if (table_id != ORLI) return 0;

  switch(table_id){
  case WARE:
    w_id = key;
    break;
    // return 0;
  case DIST:
    w_id = districtKeyToWare(key);
    break;
    // return 0;
  case CUST:
    w_id = customerKeyToWare(key);
    break;
    // return 0;
  case STOC:
    w_id = stockKeyToWare(key);
    break;
    // return 0;
  case NEWO:
    w_id = newOrderKeyToWare(key);
    break;
    // return 0;
  case ORLI:
    w_id = orderLineKeyToWare(key);
    break;
    // return 0;
  case ORDE:
    w_id = orderKeyToWare(key);
    break;
    // return 0;
  default:
    if(table_id >= 0 && table_id <= 8) return 0;
    fprintf(stdout,"recv tab %d key %lu seq %lu\n",table_id,key,seq);
    assert(false);
  }
  // return 0;
  int p_id = WarehouseToPartition(w_id);
  assert(p_id == partition_id);
  
  assert(partition_id != current_partition);

#endif
}

void ChLogCleaner::balance_index() {
  vector<int> backups = view.get_ap_backups(current_partition);

  for (int p_id : backups) {
    BackupDB *db = backup_map_[p_id];
    assert(db != nullptr);
    db->balance_index();
  }
}

void ChLogCleaner::prepare_balance(int num_workers) {
  vector<int> backups = view.get_ap_backups(current_partition);

  for (int p_id : backups) {
    BackupDB *db = backup_map_[p_id];
    assert(db != nullptr);
    db->prepare_balance(num_workers);
  }
}

void ChLogCleaner::parallel_balance(int worker_id) {
  vector<int> backups = view.get_ap_backups(current_partition);

  for (int p_id : backups) {
    BackupDB *db = backup_map_[p_id];
    assert(db != nullptr);
    db->parallel_balance(worker_id);
  }
}

void ChLogCleaner::end_balance() {
  vector<int> backups = view.get_ap_backups(current_partition);

  for (int p_id : backups) {
    BackupDB *db = backup_map_[p_id];
    assert(db != nullptr);
    db->end_balance();
  }
}

#if 0
// aborted function
void ChLogCleaner::balance_index(const vector<int> &threads) {
  vector<int> backups = view.get_ap_backups(current_partition);

  for (int p_id : backups) {
    BackupDB *db = backup_map_[p_id];
    assert(db != nullptr);
    db->balance_index(threads);
  }
}
#endif

} // namespace ch
} // namespace oltp
} // namesapce nocc
