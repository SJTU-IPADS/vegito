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

#include "app/ch/ch_query.h"

using namespace std;
using namespace livegraph;

namespace nocc {
namespace oltp {
namespace ch {


const BackupStore *c_tbl;
uint64_t c_tbl_sz;
const inline_str_fixed<2> *c_states;
const inline_str_8<16> *c_lasts;
const inline_str_8<20> *c_cities;
const inline_str_fixed<16> *c_phones;

const BackupStore *no_tbl;

const BackupStore *o_tbl;
const int8_t *o_ol_cnts;
const int32_t *o_c_ids;
const uint32_t *o_entry_ds;

BackupStore *ol_tbl;
const int8_t *ol_quantities;
const int32_t *ol_i_ids;
const AMOUNT *ol_amounts;
const int32_t *ol_supply_w_ids;

const BackupStore *s_tbl;
uint64_t s_tbl_sz;

const BackupStore *i_tbl;
uint64_t i_tbl_sz;
const inline_str_8<50> *i_datas;
const inline_str_8<24> *i_names;
const AMOUNT *i_prices;

const BackupStore *r_tbl;
const inline_str_8<15> *r_names;

const BackupStore *n_tbl;
const int8_t *n_regionkeys;
const inline_str_8<15> *n_names;

const BackupStore *su_tbl;
const int8_t *su_nationkeys;
const inline_str_fixed<15> *su_phones;
const inline_str_fixed<20> *su_names;
const inline_str_8<40> *su_addresses;
const inline_str_8<100> *su_comments;

ChQueryWorker::ChQueryWorker(uint32_t worker_id, uint32_t num_thread, 
                             uint32_t seed,
                             uint32_t start_w, uint32_t end_w,
                             BackupDB *store, graph::GraphStore *graph)
    : db_(store), graph_(graph),
      QueryWorker(worker_id, num_thread, seed),
      start_w_(start_w), end_w_(end_w) {
  if (!store && !graph) assert(false);
  // printf("[CH] Query worker %d response for ware [%d, %d)\n",
  //         worker_id, start_ware, end_ware);

  if (db_) {
    // XXX: only initialize once
    ol_tbl = db_->getStore(ORLI);
  
    const uint64_t V = 0;
    std::unique_ptr<BackupStore::ColCursor> 
      ol_quantity_cur = ol_tbl->getColCursor(OL_QUANTITY, V);
    if (!ol_quantity_cur.get()) return;
  
    c_tbl = db_->getStore(CUST);
    c_tbl_sz = c_tbl->getKeyCol().size();
    c_states = (inline_str_fixed<2> *) c_tbl->col(C_STATE);
    c_lasts = (inline_str_8<16> *) c_tbl->col(C_LAST);
    c_cities = (inline_str_8<20> *) c_tbl->col(C_CITY);
    c_phones = (inline_str_fixed<16> *) c_tbl->col(C_PHONE);
  
    no_tbl = db_->getStore(NEWO);
  
    o_tbl = db_->getStore(ORDE);
    o_ol_cnts = (int8_t *) o_tbl->col(O_OL_CNT);
    o_c_ids = (int32_t *) o_tbl->col(O_C_ID);
    o_entry_ds = (uint32_t *) o_tbl->col(O_ENTRY_D);
  
    ol_quantities = (int8_t *) ol_tbl->col(OL_QUANTITY);
    ol_i_ids = (int32_t *) ol_tbl->col(OL_I_ID);
    ol_amounts = (AMOUNT *) ol_tbl->col(OL_AMOUNT);
    ol_supply_w_ids = (int32_t *) ol_tbl->col(OL_SUPPLY_W_ID);
  
  
    s_tbl = db_->getStore(STOC);
    s_tbl_sz = s_tbl->getKeyCol().size();
  
    i_tbl = db_->getStore(ITEM);
    i_tbl_sz = i_tbl->getKeyCol().size();
    i_datas = (inline_str_8<50> *) i_tbl->col(I_DATA);
    i_names = (inline_str_8<24> *) i_tbl->col(I_NAME);
    i_prices = (AMOUNT *) i_tbl->col(I_PRICE);
  
    r_tbl = db_->getStore(REGI); 
    r_names = (inline_str_8<15> *) r_tbl->col(R_NAME);
  
    n_tbl = db_->getStore(NATI);
    n_regionkeys = (int8_t *) n_tbl->col(N_REGIONKEY);
    n_names = (inline_str_8<15> *) n_tbl->col(N_NAME);
  
    su_tbl = db_->getStore(SUPP);
    su_nationkeys = (int8_t *) su_tbl->col(SU_NATIONKEY);
    su_phones = (inline_str_fixed<15> *) su_tbl->col(SU_PHONE);
    su_names = (inline_str_fixed<20> *) su_tbl->col(SU_NAME);
    su_addresses = (inline_str_8<40> *) su_tbl->col(SU_ADDRESS);
    su_comments = (inline_str_8<100> *) su_tbl->col(SU_COMMENT);
  }
}

void ChQueryWorker::thread_local_init() {
  if (db_) {
    query02_init();
    query05_init();
    query10_init();
    query15_init();
    query16_init();
    query18_init();
  }
}

QryDescVec ChQueryWorker::get_workload() const {
  QryDescVec w;
  const vector<int> qids = chConfig.getQueryWorkload();
  QryDesc queries[] = 
    { {"", nullptr},
      
      // CH queries 
      {"Q01", Q01}, {"Q02", Q02}, {"Q03", Q03}, {"Q04", Q04},
      {"Q05", Q05}, {"Q06", Q06}, {"Q07", Q07}, {"Q08", Q08},
      {"Q09", Q09}, {"Q10", Q10}, {"Q11", Q11}, {"Q12", Q12},
      {"Q13", Q13}, {"Q14", Q14}, {"Q15", Q15}, {"Q16", Q16},
      {"Q17", Q17}, {"Q18", Q18}, {"Q19", Q19}, {"Q20", Q20},
      {"Q21", Q21}, {"Q22", Q22}, 

      // micro benchmark
      {"Q23-static", Q23}, {"Q24-update", Q24}, {"Q25-OL-update", Q25},
      {"Q26-freshness", Q26}, 
      
      // index
      {"Q27-index", Q27}, {"Q28-graph", Q28},

      // Graph HTAP micro
      {"Q29", Q29}, {"Q30", Q30},
    };

  for (int id : qids) {
    if (queries[id].fn != nullptr)
      w.push_back(queries[id]);
  }

  return w;
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

