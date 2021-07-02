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

#include "ch_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"

#include <set>
#include <boost/bind.hpp>

extern size_t current_partition;

//#define RC

namespace nocc {

namespace oltp {
namespace ch {

ChWorker::ChWorker(unsigned int worker_id, unsigned long seed,
                   uint warehouse_id_start, uint warehouse_id_end,
                   MemDB *store)
    : BenchWorker(worker_id, seed, store),
      start_w_(warehouse_id_start),
      end_w_(warehouse_id_end) {
  assert(store);
  // init last warehouse id
  for (uint w = start_w_; w < end_w_; ++w) {
    for (uint d = 0; d < NumDistrictsPerWarehouse(); ++d) {
      last_no_o_ids_[w - start_w_][d] = 3000; 
      // 3000 is magic number, which is the init num of new order
    }
  }
}

txn_result_t ChWorker::txn_micro_new_order(yield_func_t &yield) {
  const uint32_t s_op_bit = (1 << S_QUANTITY) | (1 << S_YTD) 
                      | (1 << S_REMOTE_CNT) | (1 << S_ORDER_CNT);
  // prepare input
  const uint warehouse_id = home_wid_; 
  ChNewOrderParam param(rand_gen_[cor_id_], warehouse_id);

  tx_->begin(TID_NEW_ORDER, db_logger_);
  const int32_t &districtID = param.d_id;
  const int32_t &customerID = param.c_id;
  const int32_t &numItems   = param.ol_cnt;

  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  uint64_t local_stocks[MAX_OL_CNT],   remote_stocks[MAX_OL_CNT];
  uint32_t local_item_ids[MAX_OL_CNT], remote_item_ids[MAX_OL_CNT];
  uint32_t local_supplies[MAX_OL_CNT], remote_supplies[MAX_OL_CNT];
  uint32_t local_ol_quantities[MAX_OL_CNT], remote_ol_quantities[MAX_OL_CNT];

  int num_remote_stocks(0), num_local_stocks(0);
  for (uint i = 0; i < numItems; i++) {
    uint32_t item_id = param.ol_i_ids[i];
    uint32_t supplier_warehouse_id = param.ol_supply_w_ids[i];
    uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
    uint32_t ol_quantity = param.ol_quantities[i];

    if (supplier_warehouse_id == warehouse_id) {
      // local 
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_ol_quantities[num_local_stocks] = ol_quantity;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // not local (other threads or remote threads)
      allLocal = false;

      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_ol_quantities[num_remote_stocks] = ol_quantity;
        /* First add remote read requests */
        tx_->add_to_remote_set(STOC, s_key,
                               WarehouseToPartition(supplier_warehouse_id));
        remote_item_ids[num_remote_stocks++] = item_id;
      } else {
        // remote thread, local server
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_item_ids[num_local_stocks] = item_id;
        local_ol_quantities[num_local_stocks] = ol_quantity;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  int num_servers = 0;
  if(num_remote_stocks > 0) {
    num_servers = tx_->do_remote_reads();
  }

#if 0
  uint64_t *c_value, *w_value;
  tx_->get(CUST,c_key, (char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
  checker::SanityCheckDistrict(d_value);
  //fprintf(stdout,"get dist seq %lu\n",d_seq);

  const uint64_t my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

  // tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
  // tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value), 1 << D_NEXT_O_ID);
  assert(my_next_o_id < TimeScale2);

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = param.o_entry_d;

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  // tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  // tx_->insert(ORDER_INDEX, o_sec, (char *)array_dummy,
  //             sizeof(uint64_t) + sizeof(uint64_t));
  tx_->insert_index(ORDER_INDEX, o_sec, (char *) array_dummy);
#endif

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = local_ol_quantities[ol_number - 1];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 
        (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    s_value->s_order_cnt += 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value),s_op_bit);

#if 0
    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
#if FRESHNESS == 0  // 1 for freshness
    v_ol.ol_delivery_d = 0; // not delivered yet
#else
    v_ol.ol_delivery_d = GetCurrentTimeMicro();
#endif
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
#endif
  }
#if 1
  if(num_remote_stocks > 0) {
    indirect_yield(yield);
    //yield(routines_[MASTER_ROUTINE_ID]);
    tx_->get_remote_results(num_servers);
  }
#endif

  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = remote_ol_quantities[i];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = remote_stocks[i];

    stock::value *s_value;
    uint64_t seq = tx_->get_cached(STOC,s_key,(char **)(&s_value));
    assert(seq != 0);

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
    s_value->s_order_cnt += 1;
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value),s_op_bit);

#if 0
    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
#if FRESHNESS == 0  // 1 for freshness
    v_ol.ol_delivery_d = 0; // not delivered yet
#else
    v_ol.ol_delivery_d = GetCurrentTimeMicro();
#endif
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
#endif
  }
  //fprintf(stdout,"get %d\n",my_next_o_id);
  bool res = tx_->end(yield);
  return txn_result_t(res,0);
}

txn_result_t ChWorker::txn_micro_index(yield_func_t &yield) {
  const uint32_t s_op_bit = (1 << S_QUANTITY) | (1 << S_YTD) 
                      | (1 << S_REMOTE_CNT) | (1 << S_ORDER_CNT);
  // prepare input
  const uint warehouse_id = home_wid_; 
  ChNewOrderParam param(rand_gen_[cor_id_], warehouse_id);

  tx_->begin(TID_NEW_ORDER, db_logger_);
  const int32_t &districtID = param.d_id;
  const int32_t &customerID = param.c_id;
  const int32_t &numItems   = param.ol_cnt;

  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  uint64_t local_stocks[MAX_OL_CNT],   remote_stocks[MAX_OL_CNT];
  uint32_t local_item_ids[MAX_OL_CNT], remote_item_ids[MAX_OL_CNT];
  uint32_t local_supplies[MAX_OL_CNT], remote_supplies[MAX_OL_CNT];
  uint32_t local_ol_quantities[MAX_OL_CNT], remote_ol_quantities[MAX_OL_CNT];

  int num_remote_stocks(0), num_local_stocks(0);
  for (uint i = 0; i < numItems; i++) {
    uint32_t item_id = param.ol_i_ids[i];
    uint32_t supplier_warehouse_id = param.ol_supply_w_ids[i];
    uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
    uint32_t ol_quantity = param.ol_quantities[i];

    if (supplier_warehouse_id == warehouse_id) {
      // local 
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_ol_quantities[num_local_stocks] = ol_quantity;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // not local (other threads or remote threads)
      allLocal = false;

      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_ol_quantities[num_remote_stocks] = ol_quantity;
        /* First add remote read requests */
        tx_->add_to_remote_set(STOC, s_key,
                               WarehouseToPartition(supplier_warehouse_id));
        remote_item_ids[num_remote_stocks++] = item_id;
      } else {
        // remote thread, local server
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_item_ids[num_local_stocks] = item_id;
        local_ol_quantities[num_local_stocks] = ol_quantity;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  int num_servers = 0;
  if(num_remote_stocks > 0) {
    num_servers = tx_->do_remote_reads();
  }

#if 0
  uint64_t *c_value, *w_value;
  tx_->get(CUST,c_key, (char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));
#endif

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
  checker::SanityCheckDistrict(d_value);
  //fprintf(stdout,"get dist seq %lu\n",d_seq);

  const uint64_t my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

#if 0
  tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
#endif
  tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value), 1 << D_NEXT_O_ID);
  assert(my_next_o_id < TimeScale2);

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

#if 0
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = param.o_entry_d;

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  // tx_->insert(ORDER_INDEX, o_sec, (char *)array_dummy,
  //             sizeof(uint64_t) + sizeof(uint64_t));
  tx_->insert_index(ORDER_INDEX, o_sec, (char *) array_dummy);
#endif

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = local_ol_quantities[ol_number - 1];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 
        (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    s_value->s_order_cnt += 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value),s_op_bit);

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
#if 1
  if(num_remote_stocks > 0) {
    indirect_yield(yield);
    //yield(routines_[MASTER_ROUTINE_ID]);
    tx_->get_remote_results(num_servers);
  }
#endif

  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = remote_ol_quantities[i];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = remote_stocks[i];

    stock::value *s_value;
    uint64_t seq = tx_->get_cached(STOC,s_key,(char **)(&s_value));
    assert(seq != 0);

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
    s_value->s_order_cnt += 1;
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value),s_op_bit);

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
  //fprintf(stdout,"get %d\n",my_next_o_id);
  bool res = tx_->end(yield);
  return txn_result_t(res,0);
}

txn_result_t ChWorker::txn_new_order(yield_func_t &yield) {
  const uint32_t s_op_bit = (1 << S_QUANTITY) | (1 << S_YTD) 
                      | (1 << S_REMOTE_CNT) | (1 << S_ORDER_CNT);
  // prepare input
  const uint warehouse_id = home_wid_; 
  ChNewOrderParam param(rand_gen_[cor_id_], warehouse_id);

  tx_->begin(TID_NEW_ORDER, db_logger_);
  const int32_t &districtID = param.d_id;
  const int32_t &customerID = param.c_id;
  const int32_t &numItems   = param.ol_cnt;

  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  uint64_t local_stocks[MAX_OL_CNT],   remote_stocks[MAX_OL_CNT];
  uint32_t local_item_ids[MAX_OL_CNT], remote_item_ids[MAX_OL_CNT];
  uint32_t local_supplies[MAX_OL_CNT], remote_supplies[MAX_OL_CNT];
  uint32_t local_ol_quantities[MAX_OL_CNT], remote_ol_quantities[MAX_OL_CNT];

  int num_remote_stocks(0), num_local_stocks(0);
  for (uint i = 0; i < numItems; i++) {
    uint32_t item_id = param.ol_i_ids[i];
    uint32_t supplier_warehouse_id = param.ol_supply_w_ids[i];
    uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
    uint32_t ol_quantity = param.ol_quantities[i];

    if (supplier_warehouse_id == warehouse_id) {
      // local 
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_ol_quantities[num_local_stocks] = ol_quantity;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // not local (other threads or remote threads)
      allLocal = false;

      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_ol_quantities[num_remote_stocks] = ol_quantity;
        /* First add remote read requests */
        tx_->add_to_remote_set(STOC, s_key,
                               WarehouseToPartition(supplier_warehouse_id));
        remote_item_ids[num_remote_stocks++] = item_id;
      } else {
        // remote thread, local server
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_item_ids[num_local_stocks] = item_id;
        local_ol_quantities[num_local_stocks] = ol_quantity;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }
#if 0
  {
  bool res = tx_->end(yield);
  return txn_result_t(res, 0);
  }
#endif

  int num_servers = 0;
  if(num_remote_stocks > 0) {
    num_servers = tx_->do_remote_reads();
  }

  uint64_t *c_value, *w_value;
  tx_->get(CUST,c_key, (char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
  checker::SanityCheckDistrict(d_value);
  //fprintf(stdout,"get dist seq %lu\n",d_seq);

  const uint64_t my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

  tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
  tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value), 1 << D_NEXT_O_ID);
  assert(my_next_o_id < TimeScale2);

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = param.o_entry_d;

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  // tx_->insert(ORDER_INDEX, o_sec, (char *)array_dummy,
  //             sizeof(uint64_t) + sizeof(uint64_t));
  tx_->insert_index(ORDER_INDEX, o_sec, (char *) array_dummy);

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = local_ol_quantities[ol_number - 1];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 
        (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    s_value->s_order_cnt += 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value),s_op_bit);

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
#if FRESHNESS == 0  // 1 for freshness
    v_ol.ol_delivery_d = 0; // not delivered yet
#else
    v_ol.ol_delivery_d = GetCurrentTimeMicro();
#endif
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
#if 1
  if(num_remote_stocks > 0) {
    indirect_yield(yield);
    //yield(routines_[MASTER_ROUTINE_ID]);
    tx_->get_remote_results(num_servers);
  }
#endif
#if 0
  {
  bool res = tx_->end(yield);
  return txn_result_t(res, 0);
  }
#endif

  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = remote_ol_quantities[i];

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = remote_stocks[i];

    stock::value *s_value;
    uint64_t seq = tx_->get_cached(STOC,s_key,(char **)(&s_value));
    assert(seq != 0);

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
    s_value->s_order_cnt += 1;
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value),s_op_bit);

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                       my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
#if FRESHNESS == 0  // 1 for freshness
    v_ol.ol_delivery_d = 0; // not delivered yet
#else
    v_ol.ol_delivery_d = GetCurrentTimeMicro();
#endif
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
  //fprintf(stdout,"get %d\n",my_next_o_id);
  bool res = tx_->end(yield);
#if 0
  if(res == true) {
    if(order_num.find(my_next_o_id) != order_num.end()) {
      fprintf(stdout,"exists o id %d, val %d\n",
              my_next_o_id,order_num[my_next_o_id]);
      assert(false);
    }
    //fprintf(stdout,"commit %d\n",my_next_o_id);
    // check order insertation status
    for(int32_t line_number = 1;line_number <= numItems;++line_number) {
      uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, 
                                         my_next_o_id,line_number);
      assert(store_->Get(ORLI,ol_key) != NULL);
    }
    uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);
    oorder::value *v_o = (oorder::value *)
        ((char *)(store_->Get(ORDE,o_key)) +  sizeof(_SIValHeader));
    if(v_o->o_ol_cnt != numItems) {
      fprintf(stdout,"ol cnt %d,real %d\n",v_o->o_ol_cnt,numItems);
      assert(false);
    }
    order_num.insert(std::make_pair(my_next_o_id,numItems));
  }
#endif
  return txn_result_t(res,0);
}

txn_result_t ChWorker::txn_payment(yield_func_t &yield) {

  //fprintf(stdout,"TX begin\n");
  tx_->begin(TID_PAYMENT, db_logger_);

  uint32_t warehouse_id, districtID;
  uint32_t customerWarehouseID, customerDistrictID;
  bool allLocal = true;
  uint64_t d_key;
#if 1
  //here is the trick
  customerWarehouseID = home_wid_; 
  ChPaymentParam param(rand_gen_[cor_id_], customerWarehouseID);
  customerDistrictID = param.c_d_id; 
  warehouse_id = param.w_id;
  districtID = param.d_id;
  d_key = makeDistrictKey(warehouse_id, districtID);

  int pid = WarehouseToPartition(warehouse_id);
  if(pid != current_partition) {
    allLocal = false;
    tx_->add_to_remote_set(WARE,warehouse_id,pid);
    tx_->add_to_remote_set(DIST,d_key,pid);
  }

#else
  /* here is the request generation without using DrTM's transform trick, 
   * does not work for distributed case now */
  districtID = RandomNumber(rand_gen_[cor_id_],1,NumDistrictsPerWarehouse());
  warehouse_id = 
      PickWarehouseId(rand_gen_[cor_id_],start_w_,end_w_);

  if (likely(NumWarehouses() == 1 || 
            RandomNumber(rand_gen_[cor_id_],1,100)) <= 85) {
    customerWarehouseID = warehouse_id;
    customerDistrictID  = districtID;
  } else {
    /* remote cases */
    customerDistrictID = 
        RandomNumber(rand_gen_[cor_id_],1,NumDistrictsPerWarehouse());
    do  {
      customerWarehouseID = 
          PickWarehouseId(rand_gen_[cor_id_],start_w_,end_w_);

    } while(customerWarehouseID == warehouse_id);
  }
  d_key = makeDistrictKey(warehouse_id, districtID);
#endif
  const float &paymentAmount = param.h_amount;

  /* transaction payment's execution starts */
  if(!allLocal) {
    int num_servers = tx_->do_remote_reads();
    assert(num_servers == 1);
    indirect_yield(yield);
    //yield(routines_[MASTER_ROUTINE_ID]);
    tx_->get_remote_results(num_servers);
    //          tx_->remoteset->clear_for_reads();
  }

  // find c_key and v_c
  uint64_t c_key;
  customer::value *v_c;
#if 1
  if (param.choose_by_name) {
    // cust by name
    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    const std::string &clast = param.c_last;
    uint64_t c_start = makeCustomerIndex(customerWarehouseID, 
                                         customerDistrictID, clast, zeros);
    uint64_t c_end   = makeCustomerIndex(customerWarehouseID, 
                                         customerDistrictID, clast, ones);

#ifdef  RAD_TX
    RadIterator  iter((DBRad *)tx_, CUST_INDEX, false);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_, CUST_INDEX,false);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,CUST_INDEX,false);
#endif

    iter.Seek(c_start);
    uint64_t c_keys[100] = { 0 };
    int j = 0;

    for ( ; iter.Valid() && compareCustomerIndex(iter.Key(), c_end); 
            iter.Next()) {
      uint64_t *prikeys = (uint64_t *)((char *)(iter.Value())  + META_LENGTH);
      int num = prikeys[0];
      assert(num < 16);
      for (int i = 1; i <= num; i++) {
        assert(prikeys[i] >> 32 == customerWarehouseID * 10 + customerDistrictID);
        c_keys[j] = prikeys[i];
        assert(prikeys[i] != 0);
        j++;
      }
      if (j >= 100) {
        printf("P Array Full\n");
        assert(false);
      }
    }
    j = (j+1)/2 - 1;
    c_key = c_keys[j];
  } else {
    // cust by ID
    const uint customerID = param.c_id;
    c_key = makeCustomerKey(customerWarehouseID,customerDistrictID,customerID);
  }

  uint64_t seq = tx_->get(CUST, c_key, 
                          (char **)(&v_c), sizeof(customer::value));
  if(unlikely(seq == 1)) {

    uint64_t upper = (c_key >> 32);
    fprintf(stderr,"customer not found!! %lu\n",c_key);

    int ware = upper / 10;
    int d = upper % 10;
    fprintf(stdout,"ware %d, dist %d, cust ware %d, dist %d\n",ware,d,
            customerWarehouseID,customerDistrictID);
    assert(false);
  }

  v_c->c_balance -= paymentAmount;
  v_c->c_ytd_payment += paymentAmount;
  v_c->c_payment_cnt += 1;

  if (strncmp(v_c->c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int d_id = static_cast<int32_t>(c_key >> 32) % 10;
    if (d_id == 0) d_id = 10;
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
                     static_cast<int32_t>(c_key << 32 >> 32),
                     d_id,
                     (static_cast<int32_t>(c_key >> 32) - d_id)/10,
                     districtID,
                     warehouse_id,
                     paymentAmount,
                     v_c->c_data.c_str());
    v_c->c_data.resize_junk(std::min(static_cast<size_t>(n), 
                                     v_c->c_data.max_size()));
    NDB_MEMCPY((void *) v_c->c_data.data(), &buf[0], v_c->c_data.size());
  }
  uint32_t c_op_bit = (1 << C_BALANCE) | (1 << C_YTD_PAYMENT) 
                      | (1 << C_PAYMENT_CNT) | (1 << C_DATA);
  tx_->write(CUST,c_key,(char *)v_c,sizeof(customer::value), c_op_bit);
#endif

  int d_id = static_cast<int32_t>(c_key >> 32) % 10;
  if (d_id == 0) d_id = 10;
  uint64_t h_key = makeHistoryKey(static_cast<int32_t>(c_key << 32 >> 32),
                                  d_id, 
                                  (static_cast<int32_t>(c_key >> 32)-d_id) / 10,
                                  districtID, warehouse_id);
  /* a little re-ordering, 
   * so that we can use coroutine to hide the remote fetch latency */
  warehouse::value *v_w;
  district::value *v_d;
  if(!allLocal) {
    //assert(false);
    uint64_t seq = tx_->get_cached(WARE,warehouse_id,(char **)(&v_w));
    assert(seq != 0);
    seq = tx_->get_cached(DIST,d_key,(char **)(&v_d));
    assert(seq != 0);
    //checker::SanityCheckDistrict(v_d);
    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;

    tx_->remote_write(0,(char *)v_w,sizeof(warehouse::value), 1 << W_YTD);
    tx_->remote_write(1,(char *)v_d,sizeof(district::value), 1 << D_YTD);
  } else {
    tx_->get(WARE,warehouse_id,(char **)(&v_w),sizeof(warehouse::value));
    assert(d_key != 0);
    tx_->get(DIST,d_key,(char **)(&v_d),sizeof(district::value));

    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;

    tx_->write(WARE,warehouse_id,(char *)v_w,sizeof(warehouse::value), 1 << W_YTD);
    tx_->write(DIST,d_key,(char *)v_d,sizeof(district::value), 1 << D_YTD);
  }
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s",
                   v_w->w_name.c_str(),
                   v_d->d_name.c_str());
  v_h.h_data.resize_junk(std::min(static_cast<size_t>(n), 
                                  v_h.h_data.max_size() - 1));
  v_h.h_date = param.h_date;

  const size_t history_sz = sizeof(v_h);
  ssize_t ret = 0;
  ret += history_sz;

  tx_->insert(HIST,h_key,(char *)(&v_h),sizeof(history::value));

  bool res = tx_->end(yield);
  return txn_result_t(res,73);
}

txn_result_t ChWorker::txn_delivery(yield_func_t &yield) {

  const uint warehouse_id = home_wid_; 
  ChDeliveryParam param(rand_gen_[cor_id_], warehouse_id);
  const uint32_t &o_carrier_id = param.o_carrier_id;

  tx_->begin(TID_DELIVERY, db_logger_);

  uint res = 0;

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {

    int32_t no_o_id = 1;
    uint64_t *no_value;
    int64_t start = makeNewOrderKey(warehouse_id, d, 
                                    last_no_o_ids_[warehouse_id - start_w_][d - 1]);
    int64_t end = makeNewOrderKey(warehouse_id, d, 
                                  std::numeric_limits<int32_t>::max());
    int64_t no_key  = -1;

#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,NEWO);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,NEWO);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,NEWO);
#endif
    iter.Seek(start);
    if (iter.Valid()) {

      no_key = iter.Key();

      if (no_key <= end) {
        assert(no_key >= start);
        no_o_id = static_cast<int32_t>(no_key << 32 >> 32);
        tx_->delete_(NEWO, no_key);
        assert(last_no_o_ids_[warehouse_id - start_w_][d-1] == no_o_id);
        last_no_o_ids_[warehouse_id - start_w_][d-1] = no_o_id + 1;
      }
    }

    if (no_key == -1) {
      iter.SeekToFirst();
      // Count as user inited abort: no order needed to delivery
      tx_->abort();
      return txn_result_t(true, 0);
      // skip this one
      //continue;
    }

    uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_id);
    oorder::value *o_value = NULL;

    int seq = tx_->get(ORDE, o_key, (char **)(&o_value),sizeof(oorder::value));
    if(unlikely(seq % 2 == 1 || seq == 0)) {
      continue;
    }
    assert(seq >= 2);

    float sum_ol_amount = 0;

#ifdef RAD_TX
    RadIterator iter1((DBRad *)tx_,ORLI);
#elif  defined(OCC_TX)
    DBTXIterator iter1((DBTX *)tx_,ORLI);
#elif defined(SI_TX)
    SIIterator iter1((DBSI *)tx_,ORLI);
#endif

    int ol_count;

    start = makeOrderLineKey(warehouse_id, d, no_o_id, 1);
    iter1.Seek(start);
    end = makeOrderLineKey(warehouse_id, d, no_o_id, 15);
    //      int ol_c(0);

    while (iter1.Valid()) {
      int64_t ol_key = iter1.Key();
      if (ol_key > end || ol_count >= 15) {
        break;
      } else {

      }

      order_line::value *v_ol;
      seq = tx_->get(ORLI,ol_key,(char **)(&v_ol),sizeof(order_line::value));
      assert(seq != 1);
      sum_ol_amount += v_ol->ol_amount;
      ol_count += 1;

#if FRESHNESS == 0  // 1 for freshness
      v_ol->ol_delivery_d = GetCurrentTimeMillis();
#else
      v_ol->ol_delivery_d = GetCurrentTimeMicro();
      // printf("send %lx\n", v_ol->ol_delivery_d);
#endif
      tx_->write(ORLI,ol_key,(char *)(v_ol),sizeof(order_line::value), 1 << OL_DELIVERY_D);
      iter1.Next();
    }

    o_value->o_carrier_id = o_carrier_id;
    tx_->write(ORDE,o_key,(char *)o_value,sizeof(oorder::value), 1 << O_CARRIER_ID);

    const uint c_id = o_value->o_c_id;

    const float ol_total = sum_ol_amount;
    uint64_t c_key = makeCustomerKey(warehouse_id, d, c_id);
    customer::value *v_c;

    seq = tx_->get(CUST,c_key,((char **)(&v_c)),sizeof(customer::value));
    assert(seq != 1);
    checker::SanityCheckCustomer(v_c);
    v_c->c_balance += ol_total;
    v_c->c_delivery_cnt += 1;
    uint32_t c_op_bit = (1 << C_BALANCE) | (1 << C_DELIVERY_CNT);
    tx_->write(CUST,c_key,(char *)v_c,sizeof(customer::value), c_op_bit);
    /* end iterate district per-warehouse */
  }
  bool ret = tx_->end(yield);
  return txn_result_t(ret,res);
}


txn_result_t ChWorker::txn_stock_level(yield_func_t &yield) {

  const uint warehouse_id = home_wid_; 
  ChStockLevelParam param(rand_gen_[cor_id_], warehouse_id);
  const uint32_t districtID = param.d_id;
  const uint32_t threshold = param.threshold;

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  tx_->begin();

#ifdef RC
  district::value *d_p;
  tx_->get(DIST,d_key,(char **)(&d_p),sizeof(district::value));
  tx_->write(DIST,d_key,NULL,0);
  uint64_t cur_next_o_id = d_p->d_next_o_id;
#else
  district::value v_d ;
  tx_->get_ro(DIST,d_key,(char *)(&v_d),yield);
  uint64_t cur_next_o_id = v_d.d_next_o_id;
#endif

  const int32_t lower = cur_next_o_id >= STOCK_LEVEL_ORDER_COUNT ? 
                        (cur_next_o_id - STOCK_LEVEL_ORDER_COUNT) : 0;
  uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
  uint64_t end   = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);

  std::vector<int32_t> ol_i_ids;
  std::vector<int32_t> s_i_ids;

#ifdef RAD_TX
  RadIterator  iter((DBRad *)tx_,ORLI);
#elif defined(OCC_TX)
  DBTXIterator iter((DBTX *)tx_,ORLI);
#elif defined(SI_TX)
  SIIterator iter((DBSI *)tx_,ORLI);
#endif
  iter.Seek(start);

  for ( ;iter.Valid(); iter.Next()) {

    uint64_t ol_key = iter.Key();

    if (ol_key >= end) break;

    //uint64_t *ol_value = (uint64_t *)((char *)(iter.Value()) + META_LENGTH);
    //      order_line::value *v_ol = (order_line::value *)ol_value;
    order_line::value v_ol;
    if(unlikely(tx_->get_ro(ORLI,ol_key,(char *)(&v_ol),yield) == 0)) {
      continue;  // a dirty value, XXX: a more efficient method? directly use node seq
    }

    int64_t s_key = makeStockKey(warehouse_id, v_ol.ol_i_id);

#ifdef RC
    stock::value *s_p;
    tx_->get(STOC,s_key,(char **)(&s_p), sizeof(stock::value));
    tx_->write(STOC,s_key,NULL,0);
    if(s_p->s_quantity < int(threshold)) {
      s_i_ids.push_back(s_key);
    }
#else
    stock::value v_s;
    tx_->get_ro(STOC,s_key,(char *)(&v_s),yield);
    if(v_s.s_quantity < int(threshold)) {
      s_i_ids.push_back(s_key);
    }
#endif
  }
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
  return txn_result_t(ret,s_i_ids.size() + ol_i_ids.size());
#else
  return txn_result_t(true,s_i_ids.size() + ol_i_ids.size());
#endif
}

txn_result_t ChWorker::txn_order_status(yield_func_t &yield) {

  const uint32_t warehouse_id = home_wid_; 
  ChOrderStatusParam param(rand_gen_[cor_id_], warehouse_id);
  const uint32_t &districtID = param.d_id;
  uint64_t c_key;
  customer::value v_c;

  tx_->begin();

  if (param.choose_by_name) {
    //1.1 cust by name
    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    const std::string &clast = param.c_last;
    uint64_t c_start = makeCustomerIndex(warehouse_id, districtID, clast, zeros);
    uint64_t c_end = makeCustomerIndex(warehouse_id, districtID, clast, ones);

#ifdef RAD_TX
    RadIterator  citer((DBRad *)tx_,CUST_INDEX ,false);
#elif  defined(OCC_TX)
    DBTXIterator citer((DBTX *)tx_, CUST_INDEX,false);
#elif defined(SI_TX)
    SIIterator citer((DBSI *)tx_,CUST_INDEX,false);
#endif

    citer.Seek(c_start);

    uint64_t *c_values[100];
    uint64_t c_keys[100];
    int j = 0;
    while (citer.Valid()) {

      if (compareCustomerIndex(citer.Key(), c_end)) {

        uint64_t *prikeys = (uint64_t *)((char *)(citer.Value()) + META_LENGTH);

        int num = prikeys[0];

        for (int i=1; i <= num; i++) {
          c_keys[j] = prikeys[i];
          j++;
        }

        if (j >= 100) {
          printf("OS Array Full\n");
          assert(false);
        }
      }
      else {
        break;
      }
      citer.Next();
    }

    j = (j+1)/2 - 1;
    c_key = c_keys[j];
  } else {
    //1.2 cust by ID
    const uint32_t &customerID = param.c_id;
    c_key = makeCustomerKey(warehouse_id,districtID,customerID);
  }
#ifdef RC
    customer::value *c_p;
    tx_->get(CUST,c_key,(char **)(&c_p),sizeof(customer::value));
    tx_->write(CUST,c_key,NULL,sizeof(customer::value));
#else
    tx_->get_ro(CUST,c_key,(char *)(&v_c),yield);
#endif

  //STEP 2. read record from ORDER
  int32_t o_id = -1;
  int o_ol_cnt;
#ifdef RAD_TX
  RadIterator  iter((DBRad *)tx_,ORDER_INDEX);
#elif defined(OCC_TX)
  DBTXIterator iter((DBTX *)tx_,ORDER_INDEX);
#elif defined(SI_TX)
  SIIterator iter((DBSI *)tx_,ORDER_INDEX);
#endif

  int32_t c_id = static_cast<int32_t>(c_key << 32 >> 32);

  // XXX: here is a hack to fix bug on seek
  // if we have a interface called SeekPrev, we can fix this bug really
  if (warehouse_id == end_w_ - 1 && districtID == NumDistrictsPerWarehouse()
        && c_id == NumCustomersPerDistrict()) {
    --c_id;
  }
  uint64_t start = makeOrderIndex(warehouse_id, districtID, c_id, 10000000+ 1);
  uint64_t end = makeOrderIndex(warehouse_id, districtID, c_id, 1);


  iter.Seek(start);
  if(iter.Valid())
    iter.Prev();
  else printf("ERROR: SeekOut!!!\n");

  if (iter.Valid() && iter.Key() >= end) {

    uint64_t *prikeys = (uint64_t *)((char *)(iter.Value()) + META_LENGTH);
    o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);

    oorder::value *ol_p;
#ifdef RC
    uint64_t o_seq = tx_->get(ORDE, prikeys[1],(char **)(&ol_p),sizeof(oorder::value));
    tx_->write(ORDE,prikeys[1],NULL,0);
    o_ol_cnt = ol_p->o_ol_cnt;
#else
    oorder::value v_ol;
    uint64_t od_seq = tx_->get_ro(ORDE,prikeys[1],(char *)(&v_ol),yield);
    checker::SanityCheckOOrder(&v_ol);
    o_ol_cnt = v_ol.o_ol_cnt;
#endif
    if(od_seq == 0) {
#ifdef OCC
      assert(false);
#endif
      goto END;
    }
    //    assert(false);

    //STEP 3. read record from ORDERLINE
    if (o_id != -1) {

      for (int32_t line_number = 1; line_number <= o_ol_cnt; ++line_number) {
        uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, o_id, line_number);

#ifdef      RC
        order_line::value *ol_p;
        tx_->get(ORLI,ol_key,(char **)(&ol_p),sizeof(order_line::value));
        tx_->write(ORLI,ol_key,NULL,0);
#else
        order_line::value ol_val;
        uint64_t o_seq = tx_->get_ro(ORLI,ol_key,(char *)(&ol_val),yield);
#endif
        if(unlikely(o_seq == 0)) {
          // checks
          // fprintf(stdout,"o_id %d line %d, total %d order's seq %lu\n",o_id,line_number,o_ol_cnt,od_seq);
          // assert(order_num.find(o_id) != order_num.end());
          // fprintf(stdout,"real items %d\n",order_num[o_id]);
          // assert(store_->Get(ORLI,ol_key) != NULL);
          // XXX: here is a bug!!!
          // assert(false);
          // break;
        }
      }
    }
    else {
      printf("ERROR: Customer %d No order Found\n", static_cast<int32_t>(c_key << 32 >> 32));
    }
  }
END:
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
  // ret = true;  // XXX: here is a bug
  return txn_result_t(ret,o_ol_cnt);
#else
  return txn_result_t(true,o_ol_cnt);
#endif
}

workload_desc_vec_t ChWorker::get_workload() const {
  workload_desc_vec_t w;

  int pct = chConfig.getTxnWorkloadMix(ChConfig::NEW_ORDER);
  if (pct)
    w.emplace_back("NewOrder", double(pct) / 100.0, TxnNewOrder);

  pct = chConfig.getTxnWorkloadMix(ChConfig::PAYMENT);
  if (pct)
    w.emplace_back("Payment", double(pct) / 100.0, TxnPayment);

  pct = chConfig.getTxnWorkloadMix(ChConfig::DELIVERY);
  if (pct)
    w.emplace_back("Delivery", double(pct) / 100.0, TxnDelivery);

  pct = chConfig.getTxnWorkloadMix(ChConfig::ORDER_STATUS);
  if (pct)
    w.emplace_back("OrderStatus", double(pct) / 100.0, TxnOrderStatus);

  pct = chConfig.getTxnWorkloadMix(ChConfig::STOCK_LEVEL);
  if (pct)
    w.emplace_back("StockLevel", double(pct) / 100.0, TxnStockLevel);

  return w;
}

void ChWorker::check_consistency() {

  fprintf(stdout,"[CH]: check consistency.\n");
  return;
  {
    // check the new order insert status
    tx_->begin();
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,NEWO);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,NEWO);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,NEWO);
#endif
    for(uint w = start_w_; w < end_w_;++w) {
      for(uint d = 1; d <= NumDistrictsPerWarehouse();++d) {
        //int64_t start = makeNewOrderKey(warehouse_id, d, 0);
        int64_t end = makeNewOrderKey(w, d, std::numeric_limits<int32_t>::max());
        iter.Seek(end);
        iter.Prev();
        assert(iter.Valid());
        // parse the no id from the key
        uint64_t key = iter.Key();
        uint64_t d_id = key & 0xffffffff;
        uint64_t d_key = makeDistrictKey(w, d);
        district::value *d_value;
        tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
        checker::SanityCheckDistrict(d_value);

        const uint64_t my_next_o_id = d_value->d_next_o_id;

        fprintf(stdout,"[CH] ware %d, dis %d, id %lu,next entry in district %lu\n",w,d,d_id,my_next_o_id);
        fprintf(stdout,"[CH] last processed %lu\n",last_no_o_ids_[w-start_w_][d-1]);

        assert(((key >> 32) & 0xffffffff) == w * 10 + d);
      }
    }
    // end check new order
  }

}
    
}  // namespace ch
}  // namespace oltp
}  // namespace nocc
