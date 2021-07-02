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

#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"

#include <set>

using namespace std;

namespace {

void convertString(char *newstring, const char *oldstring, int size) {
  for (int i = 0; i < 8; i++)
    if (i < size)
      newstring[7 -i] = oldstring[i];
    else newstring[7 -i] = '\0';

  for (int i=8; i<16; i++)
    if (i < size)
      newstring[23 -i] = oldstring[i];
    else newstring[23 -i] = '\0';
}

} // end anonymous namespace

namespace nocc {

using namespace db;

namespace oltp {
namespace ch {


bool compareCustomerIndex(uint64_t key, uint64_t bound) {
  uint64_t *k = (uint64_t *)key;
  uint64_t *b = (uint64_t *)bound;
  for (int i=0; i < 5; i++) {
    if (k[i] > b[i]) return false;
    if (k[i] < b[i]) return true;
  }
  return true;
}

uint64_t
makeCustomerIndex(int32_t w_id, int32_t d_id, string s_last, string s_first) {
  uint64_t *seckey = new uint64_t[5];
  int32_t did = d_id + (w_id * 10);
  seckey[0] = did;
  convertString((char *)(&seckey[1]), s_last.data(), s_last.size());
  convertString((char *)(&seckey[3]), s_first.data(), s_last.size());
  return (uint64_t)seckey;
}

void ChWarehouseLoader::load() {
  // printf("\n**** [CH] Loading data ****\n");
  uint64_t warehouse_total_sz = 0, n_warehouses = 0;
  try {
    for (uint i = GetStartWarehouse(partition_);
         i <= GetEndWarehouse(partition_); i++) {

      const warehouse::key k{i};
      const string w_name = RandomStr(rand_,
                                      RandomNumber(rand_, 6, 10));
      const string w_street_1 = RandomStr(
                                  rand_,
                                  RandomNumber(rand_, 10, 20));
      const string w_street_2 = RandomStr(
                                  rand_,
                                  RandomNumber(rand_, 10, 20));
      const string w_city = RandomStr(
                              rand_,
                              RandomNumber(rand_, 10, 20));
      const string w_state = RandomStr(rand_, 3);
      const string w_zip = "123456789";

      int w_size = store_->_schemas[WARE].total_len;

      char *wrapper = (char *)malloc(w_size);
      memset(wrapper, 0,
             META_LENGTH + sizeof(warehouse::value) + sizeof(uint64_t));
      warehouse::value *v = (warehouse::value *)(wrapper + META_LENGTH);
      v->w_ytd = 300000 * 100;
      v->w_tax = (float) RandomNumber(rand_, 0, 2000) / 10000.0;
      v->w_name.assign(w_name);
      v->w_street_1.assign(w_street_1);
      v->w_street_2.assign(w_street_2);
      v->w_city.assign(w_city);
      v->w_state.assign(w_state);
      v->w_zip.assign(w_zip);

      checker::SanityCheckWarehouse(&k, v);
      const size_t sz = sizeof(*v);
      warehouse_total_sz += sz;
      n_warehouses++;

      store_->Put(WARE, i, (uint64_t *)wrapper);

      if (sizeof(k) !=8)
        std::cerr << sizeof(k) << std::endl;
    }
  } catch (...) {
    // shouldn't abort on loading!
    ALWAYS_ASSERT(false);
  }

  printf("[CH] finished loading warehouse\n"
         "[CH]   * average warehouse record length: %lf bytes\n",
         double(warehouse_total_sz) / double(n_warehouses));
  /* end loading warehouse */
}

void ChWarehouseLoader::loadBackup() {
  // printf("\n**** [CH] Loading data ****\n");
  assert(backup_store_ != NULL && store_ == NULL);
  uint64_t warehouse_total_sz = 0, n_warehouses = 0;
  for (uint i = GetStartWarehouse(partition_);
       i <= GetEndWarehouse(partition_); i++) {

    const warehouse::key k{i};
    const string w_name = RandomStr(rand_,
                                    RandomNumber(rand_, 6, 10));
    const string w_street_1 = RandomStr(
                                rand_,
                                RandomNumber(rand_, 10, 20));
    const string w_street_2 = RandomStr(
                                rand_,
                                RandomNumber(rand_, 10, 20));
    const string w_city = RandomStr(
                            rand_,
                            RandomNumber(rand_, 10, 20));
    const string w_state = RandomStr(rand_, 3);
    const string w_zip = "123456789";

    warehouse::value v;
    v.w_ytd = 300000 * 100;
    v.w_tax = (float) RandomNumber(rand_, 0, 2000) / 10000.0;
    v.w_name.assign(w_name);
    v.w_street_1.assign(w_street_1);
    v.w_street_2.assign(w_street_2);
    v.w_city.assign(w_city);
    v.w_state.assign(w_state);
    v.w_zip.assign(w_zip);

    const size_t sz = sizeof(v);
    warehouse_total_sz += sz;
    n_warehouses++;

    backup_store_->Insert(WARE, i, (char *) &v);
  }

  printf("[CH] finished loading warehouse\n"
         "[CH]   * average warehouse record length: %lf bytes\n",
         double(warehouse_total_sz) / double(n_warehouses));
  /* end loading warehouse */
}

void ChDistrictLoader::load() {
  int64_t district_total_sz = 0, n_districts = 0;
  try {
    uint cnt = 0;
    for (uint w = GetStartWarehouse(partition_);
              w <= GetEndWarehouse(partition_); w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
        uint64_t key = makeDistrictKey(w, d);
        const district::key k{key};

        int d_size = store_->_schemas[DIST].total_len;
        char *wrapper = (char *)malloc(d_size);
        memset(wrapper, 0,
               META_LENGTH + sizeof(district::value) + sizeof(uint64_t));
        district::value *v = (district::value *)(wrapper + META_LENGTH);
        v->d_ytd = 30000 * 100; //notice i did the scale up
        v->d_tax = (float) (RandomNumber(rand_, 0, 2000) / 10000.0);
        v->d_next_o_id = 3001;
        v->d_name.assign(RandomStr(rand_,
                                   RandomNumber(rand_, 6, 10)));
        v->d_street_1.assign(RandomStr(rand_,
                                       RandomNumber(rand_, 10, 20)));
        v->d_street_2.assign(RandomStr(rand_,
                                       RandomNumber(rand_, 10, 20)));
        v->d_city.assign(RandomStr(rand_,
                                   RandomNumber(rand_, 10, 20)));
        v->d_state.assign(RandomStr(rand_, 3));
        v->d_zip.assign("123456789");

        const size_t sz = sizeof(*v);
        district_total_sz += sz;
        n_districts++;

        store_->Put(DIST, key, (uint64_t *)wrapper);
      }
    }
  } catch (...) {
    // shouldn't abort on loading!
    ALWAYS_ASSERT(false);
  }

  printf("[CH] finished loading district\n"
         "[CH]   * average district record length: %lf bytes\n",
         double(district_total_sz) / double(n_districts));
}

void ChDistrictLoader::loadBackup() {
  int64_t district_total_sz = 0, n_districts = 0;
  uint cnt = 0;
  for (uint w = GetStartWarehouse(partition_);
            w <= GetEndWarehouse(partition_); w++) {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
      uint64_t key = makeDistrictKey(w, d);

      district::value val;
      district::value *v = &val;
      v->d_ytd = 30000 * 100; //notice i did the scale up
      v->d_tax = (float) (RandomNumber(rand_, 0, 2000) / 10000.0);
      v->d_next_o_id = 3001;
      v->d_name.assign(RandomStr(rand_,
                                 RandomNumber(rand_, 6, 10)));
      v->d_street_1.assign(RandomStr(rand_,
                                     RandomNumber(rand_, 10, 20)));
      v->d_street_2.assign(RandomStr(rand_,
                                     RandomNumber(rand_, 10, 20)));
      v->d_city.assign(RandomStr(rand_,
                                 RandomNumber(rand_, 10, 20)));
      v->d_state.assign(RandomStr(rand_, 3));
      v->d_zip.assign("123456789");

      const size_t sz = sizeof(*v);
      district_total_sz += sz;
      n_districts++;

      backup_store_->Insert(DIST, key, (char *) v);
    }
  }

  printf("[CH] finished loading district\n"
         "[CH]   * average district record length: %lf bytes\n",
         double(district_total_sz) / double(n_districts));
}

void ChCustomerLoader::load() {
  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  const size_t batchsize =
    NumCustomersPerDistrict() ;

  const size_t nbatches =
    (batchsize > NumCustomersPerDistrict()) ?
    1 : (NumCustomersPerDistrict() / batchsize);

  uint64_t total_sz = 0;
  int w = 0;
  for (w = w_start; w <= w_end; w++) {
    //      if (pin_cpus)
    //        PinToWarehouseId(w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      for (uint batch = 0; batch < nbatches;) {
        //          scoped_str_arena s_arena(arena);
        const size_t cstart = batch * batchsize;
        const size_t cend = std::min((batch + 1) * batchsize,
                                     NumCustomersPerDistrict());
        try {
          for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
            const uint c = cidx0 + 1;
            uint64_t key = makeCustomerKey(w, d, c);
            const customer::key k{key};

            int c_size = store_->_schemas[CUST].total_len;
            char *wrapper = (char *)malloc(c_size);
            memset(wrapper, 0,
                   META_LENGTH + sizeof(customer::value) + sizeof(uint64_t));
            customer::value *v = (customer::value *)(wrapper + META_LENGTH);
            v->c_discount =
                (float) (RandomNumber(rand_, 1, 5000) / 10000.0);
            if (RandomNumber(rand_, 1, 100) <= 10)
              v->c_credit.assign("BC");
            else
              v->c_credit.assign("GC");

            if (c <= 1000)
              v->c_last.assign(GetCustomerLastName(rand_, c - 1));
            else
              v->c_last.assign(
                  GetNonUniformCustomerLastNameLoad(rand_));

            v->c_first.assign(
                RandomStr(rand_,
                          RandomNumber(rand_, 8, 16)));
            v->c_credit_lim = 50000;

            v->c_balance = -10;
            v->c_ytd_payment = 10;
            v->c_payment_cnt = 1;
            v->c_delivery_cnt = 0;

            v->c_street_1.assign(
               RandomStr(rand_,
                         RandomNumber(rand_, 10, 20)));
            v->c_street_2.assign(
               RandomStr(rand_,
                         RandomNumber(rand_, 10, 20)));
            v->c_city.assign(
                RandomStr(rand_,
                          RandomNumber(rand_, 10, 20)));
            v->c_state.assign(RandomStr(rand_, 3));
            v->c_zip.assign(RandomNStr(rand_, 4) + "11111");
            v->c_phone.assign(RandomNStr(rand_, 16));
            v->c_since = GetCurrentTimeMillis();
            v->c_middle.assign("OE");
            v->c_data.assign(
                RandomStr(rand_,
                          RandomNumber(rand_, 300, 500)));

            const size_t sz = sizeof(*v);
            total_sz += sz;

            store_->Put(CUST, key, (uint64_t *)wrapper);
            // customer name index

            uint64_t sec = makeCustomerIndex(w, d,
                                             v->c_last.str(true),
                                             v->c_first.str(true));

            //    uint64_t* mn = store_->_indexs[CUST_INDEX].Get(sec);
            uint64_t *mn = store_->Get(CUST_INDEX,sec);
            if (mn == NULL) {
              char *ciwrap =
                  new char[META_LENGTH + sizeof(uint64_t)*2  + sizeof(uint64_t)];
              memset(ciwrap, 0, META_LENGTH);
              uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);

              prikeys[0] = 1; prikeys[1] = key;
              //printf("key %ld\n",key);
              store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
            }
            else {
              assert(false);
              printf("ccccc\n");
              uint64_t *value = (uint64_t *)((char *)mn + META_LENGTH);
              int num = value[0];
              char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*(num+2)];
              memset(ciwrap, 0, META_LENGTH);
              uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);
              prikeys[0] = num + 1;
              for (int i=1; i<=num; i++)
                prikeys[i] = value[i];
              prikeys[num+1] = key;
              store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
              //delete[] value;
            }
            char *hwrap = new char[META_LENGTH + sizeof(history::value)];
            memset(hwrap, 0, META_LENGTH);

            uint64_t hkey = makeHistoryKey(c,d,w,d,w);
            history::key k_hist{ hkey };

            history::value *v_hist = (history::value*)(hwrap + META_LENGTH);
            v_hist->h_amount = 10;
            v_hist->h_data.assign(
                RandomStr(rand_,
                          RandomNumber(rand_, 10, 24)));

            store_->Put(HIST, hkey, (uint64_t *)hwrap);
            if (sizeof(k_hist) != 8)
              cerr << sizeof(k_hist) << endl;
          }
          batch++;

        } catch (...) {
          printf("[WARNING] customer loader loading abort\n");
        }
      }
    }
    /* end iterating warehosue */
  }    
  
  double avgLen = double(total_sz) /
                     double(w * NumDistrictsPerWarehouse()
                                                  * NumCustomersPerDistrict());
  printf("[CH] finished loading customer\n"
         "[CH]   * average customer record length: %lf bytes\n",
         avgLen);
}

void ChCustomerLoader::loadBackup() {
  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  const size_t batchsize = NumCustomersPerDistrict() ;

  const size_t nbatches = (batchsize > NumCustomersPerDistrict()) ?
                          1 : (NumCustomersPerDistrict() / batchsize);

  uint64_t total_sz = 0;
  int w = 0;
  for (w = w_start; w <= w_end; w++) {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      for (uint batch = 0; batch < nbatches; batch++) {
        const size_t cstart = batch * batchsize;
        const size_t cend = std::min((batch + 1) * batchsize,
                                     NumCustomersPerDistrict());
        for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
          const uint c = cidx0 + 1;
          uint64_t key = makeCustomerKey(w, d, c);

          customer::value val;
          customer::value *v = &val;
          v->c_discount =
              (float) (RandomNumber(rand_, 1, 5000) / 10000.0);
          if (RandomNumber(rand_, 1, 100) <= 10)
            v->c_credit.assign("BC");
          else
            v->c_credit.assign("GC");

          if (c <= 1000)
            v->c_last.assign(GetCustomerLastName(rand_, c - 1));
          else
            v->c_last.assign(
                GetNonUniformCustomerLastNameLoad(rand_));

          v->c_first.assign(
              RandomStr(rand_,
                        RandomNumber(rand_, 8, 16)));
          v->c_credit_lim = 50000;

          v->c_balance = -10;
          v->c_ytd_payment = 10;
          v->c_payment_cnt = 1;
          v->c_delivery_cnt = 0;

          v->c_street_1.assign(
             RandomStr(rand_,
                       RandomNumber(rand_, 10, 20)));
          v->c_street_2.assign(
             RandomStr(rand_,
                       RandomNumber(rand_, 10, 20)));
          v->c_city.assign(
              RandomStr(rand_,
                        RandomNumber(rand_, 10, 20)));
          v->c_state.assign(RandomStr(rand_, 3));
          v->c_zip.assign(RandomNStr(rand_, 4) + "11111");
          v->c_phone.assign(RandomNStr(rand_, 16));
          v->c_since = GetCurrentTimeMillis();
          v->c_middle.assign("OE");
          v->c_data.assign(
              RandomStr(rand_,
                        RandomNumber(rand_, 300, 500)));

          const size_t sz = sizeof(*v);
          total_sz += sz;

          backup_store_->Insert(CUST, key, (char *) v);

#if 0  // load secondary index (customer name)
          uint64_t sec = makeCustomerIndex(w, d,
                                           v->c_last.str(true),
                                           v->c_first.str(true));

          uint64_t *mn = store_->Get(CUST_INDEX,sec);
          if (mn == NULL) {
            char *ciwrap =
                new char[META_LENGTH + sizeof(uint64_t)*2  + sizeof(uint64_t)];
            memset(ciwrap, 0, META_LENGTH);
            uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);

            prikeys[0] = 1; prikeys[1] = key;
            //printf("key %ld\n",key);
            store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
          }
          else {
            assert(false);
            printf("ccccc\n");
            uint64_t *value = (uint64_t *)((char *)mn + META_LENGTH);
            int num = value[0];
            char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*(num+2)];
            memset(ciwrap, 0, META_LENGTH);
            uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);
            prikeys[0] = num + 1;
            for (int i=1; i<=num; i++)
              prikeys[i] = value[i];
            prikeys[num+1] = key;
            store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
            //delete[] value;
          }
#endif
          uint64_t hkey = makeHistoryKey(c,d,w,d,w);
          history::value v_hist;
          v_hist.h_amount = 10;
          v_hist.h_data.assign(
              RandomStr(rand_,
                        RandomNumber(rand_, 10, 24)));

          backup_store_->Insert(HIST, hkey, (char *) &v_hist);
        }
      }
    }  /* end iterating warehosue */
  }

  double avgLen = double(total_sz) /
                     double(w * NumDistrictsPerWarehouse()
                                                  * NumCustomersPerDistrict());
  printf("[CH] finished loading customer\n"
         "[CH]   * average customer record length: %lf bytes\n",
         avgLen);
}

void ChOrderLoader::load() {
  uint64_t order_line_total_sz = 0, n_order_lines = 0;
  uint64_t oorder_total_sz = 0, n_oorders = 0;
  uint64_t new_order_total_sz = 0, n_new_orders = 0;

  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    //      if (pin_cpus)
    //        PinToWarehouseId(w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      set<uint> c_ids_s;
      vector<uint> c_ids;
      while (c_ids.size() != NumCustomersPerDistrict()) {
        const uint x =
            (rand_.next() % NumCustomersPerDistrict()) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }
      for (uint c = 1; c <= NumCustomersPerDistrict();) {
        try {
          uint64_t okey = makeOrderKey(w, d, c);
          const oorder::key k_oo { okey };

          char *wrapper =
              new char[META_LENGTH+sizeof(oorder::value) + sizeof(uint64_t)];
          memset(wrapper, 0 ,
                 META_LENGTH + sizeof(oorder::value) + sizeof(uint64_t));
          oorder::value *v_oo = (oorder::value *)(wrapper + META_LENGTH);
          v_oo->o_c_id = c_ids[c - 1];
          if (c < 2101)
            v_oo->o_carrier_id = RandomNumber(rand_, 1, 10);
          else
            v_oo->o_carrier_id = 0;
          v_oo->o_ol_cnt = RandomNumber(rand_, 5, 15);

          v_oo->o_all_local = 1;
          v_oo->o_entry_d = GetCurrentTimeMillis();
          //        checker::SanityCheckOOrder(&k_oo, v_oo);
          const size_t sz = sizeof(*v_oo);
          oorder_total_sz += sz;
          n_oorders++;
          store_->Put(ORDE, okey, (uint64_t *)wrapper);

          uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);

          char *oiwrapper = new char[META_LENGTH+16+sizeof(uint64_t)];
          memset(oiwrapper, 0 ,META_LENGTH + 16 + sizeof(uint64_t));
          uint64_t *prikeys = (uint64_t *)(oiwrapper+META_LENGTH);
          prikeys[0] = 1; prikeys[1] = okey;
          store_->Put(ORDER_INDEX, sec, (uint64_t *)oiwrapper);

          if (c >= 2101) {
            uint64_t nokey = makeNewOrderKey(w, d, c);
            const new_order::key k_no { nokey };

            char* nowrap =
                new char[META_LENGTH + sizeof(new_order::value) + sizeof(uint64_t)];
            memset(nowrap, 0,
                   META_LENGTH + sizeof(new_order::value) + sizeof(uint64_t));
            new_order::value *v_no = (new_order::value *)(nowrap+META_LENGTH );

            checker::SanityCheckNewOrder(&k_no);
            const size_t sz = sizeof(*v_no);
            new_order_total_sz += sz;
            n_new_orders++;
            store_->Put(NEWO, nokey, (uint64_t *)nowrap);
          }

#if 1
          for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
            uint64_t olkey = makeOrderLineKey(w, d, c, l);
            const order_line::key k_ol { olkey };

            char *olwrapper =
                new char[META_LENGTH+sizeof(order_line::value) + sizeof(uint64_t)];
            memset(olwrapper, 0,
                   META_LENGTH + sizeof(order_line::value) + sizeof(uint64_t));
            order_line::value *v_ol =
                (order_line::value *)(olwrapper + META_LENGTH);
            v_ol->ol_i_id = RandomNumber(rand_, 1, 100000);
            if (c < 2101) {
              v_ol->ol_delivery_d = v_oo->o_entry_d;
              v_ol->ol_amount = 0;
            } else {
              v_ol->ol_delivery_d = 0;
              /* random within [0.01 .. 9,999.99] */
              v_ol->ol_amount =
                  (float) (RandomNumber(rand_, 1, 999999) / 100.0);
            }

            v_ol->ol_supply_w_id = w;
            v_ol->ol_quantity = 5;
            // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
            checker::SanityCheckOrderLine(&k_ol, v_ol);
            const size_t sz = sizeof(*v_ol);
            order_line_total_sz += sz;
            n_order_lines++;
            store_->Put(ORLI, olkey, (uint64_t *)olwrapper);
          }
#endif
          c++;
        } catch (...) {
          printf("[WARNING] order loader loading abort\n");
        }
      }
    }

  }

  /* order loader */
}

void ChOrderLoader::loadBackup() {
  uint64_t order_line_total_sz = 0, n_order_lines = 0;
  uint64_t oorder_total_sz = 0, n_oorders = 0;
  uint64_t new_order_total_sz = 0, n_new_orders = 0;

  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      set<uint> c_ids_s;
      vector<uint> c_ids;
      while (c_ids.size() != NumCustomersPerDistrict()) {
        const uint x =
            (rand_.next() % NumCustomersPerDistrict()) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }

      for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
        uint64_t okey = makeOrderKey(w, d, c);
        const oorder::key k_oo { okey };

        oorder::value val_oo;
        oorder::value *v_oo = &val_oo;
        v_oo->o_c_id = c_ids[c - 1];
        if (c < 2101)
          v_oo->o_carrier_id = RandomNumber(rand_, 1, 10);
        else
          v_oo->o_carrier_id = 0;
        v_oo->o_ol_cnt = RandomNumber(rand_, 5, 15);

        v_oo->o_all_local = 1;
        v_oo->o_entry_d = GetCurrentTimeMillis();
        const size_t sz = sizeof(*v_oo);
        oorder_total_sz += sz;
        n_oorders++;
        backup_store_->Insert(ORDE, okey, (char *) v_oo);

#if 0 // secondary index
        uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);
        char *oiwrapper = new char[META_LENGTH+16+sizeof(uint64_t)];
        memset(oiwrapper, 0 ,META_LENGTH + 16 + sizeof(uint64_t));
        uint64_t *prikeys = (uint64_t *)(oiwrapper+META_LENGTH);
        prikeys[0] = 1; prikeys[1] = okey;
        store_->Put(ORDER_INDEX, sec, (uint64_t *)oiwrapper);
#endif

        // new order
        if (c >= 2101) {
          uint64_t nokey = makeNewOrderKey(w, d, c);
          new_order::value v_no;
          const size_t sz = sizeof(v_no);
          new_order_total_sz += sz;
          n_new_orders++;
          backup_store_->Insert(NEWO, nokey, (char *) &v_no);
        }

        // order line
        for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
          uint64_t olkey = makeOrderLineKey(w, d, c, l);

          order_line::value val_ol;
          order_line::value *v_ol = &val_ol;
          v_ol->ol_i_id = RandomNumber(rand_, 1, 100000);
          if (c < 2101) {
            v_ol->ol_delivery_d = v_oo->o_entry_d;
            assert(v_ol->ol_delivery_d != 0);
            v_ol->ol_amount = 0;
          } else {
            v_ol->ol_delivery_d = 0;
            /* random within [0.01 .. 9,999.99] */
            v_ol->ol_amount =
                (float) (RandomNumber(rand_, 1, 999999) / 100.0);
          }

          v_ol->ol_supply_w_id = w;
          v_ol->ol_quantity = 5;
          // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
          const size_t sz = sizeof(*v_ol);
          order_line_total_sz += sz;
          n_order_lines++;
          backup_store_->Insert(ORLI, olkey, (char *) v_ol);
        }
      }
    }

  }
#if BTREE_COUNT == 1  // 1 for verfiy btree index
  if (backup_store_->verify_sec_index()) {
    printf("Secondary index verified!\n");
  }
#endif
  printf("n_order_lines = %lu\n", n_order_lines);

  /* order loader */
}

void ChStockLoader::load() {

  uint64_t stock_total_sz = 0, n_stocks = 0;
  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    const size_t batchsize =  NumItems() ;
    const size_t nbatches = (batchsize > NumItems()) ?
                                1 : (NumItems() / batchsize);

    //      if (pin_cpus)
    //        PinToWarehouseId(w);

    for (uint b = 0; b < nbatches;) {
      //        scoped_str_arena s_arena(arena);
      try {
        const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
        for (uint i = (b * batchsize + 1); i <= iend; i++) {
          uint64_t key = makeStockKey(w, i);

          int s_size = store_->_schemas[STOC].total_len;
          char *wrapper = (char *)malloc(s_size);
          memset(wrapper, 0,
                 META_LENGTH + sizeof(stock::value) + sizeof(uint64_t));
          stock::value *v = (stock::value *)(wrapper + META_LENGTH);
          v->s_quantity = RandomNumber(rand_, 10, 100);
          v->s_ytd = 0;
          v->s_order_cnt = 0;
          v->s_remote_cnt = 0;

          const int len = RandomNumber(rand_, 26, 50);
          if (RandomNumber(rand_, 1, 100) > 10) {
            const string s_data = RandomStr(rand_, len);
          } else {
            const int startOriginal =
                RandomNumber(rand_, 2, (len - 8));
            const string s_data =
                RandomStr(rand_, startOriginal + 1)
                + "ORIGINAL"
                + RandomStr(rand_, len - startOriginal - 7);
          }
          /*
            v_data.s_dist_01.assign(RandomStr(r, 24));
            v_data.s_dist_02.assign(RandomStr(r, 24));
            v_data.s_dist_03.assign(RandomStr(r, 24));
            v_data.s_dist_04.assign(RandomStr(r, 24));
            v_data.s_dist_05.assign(RandomStr(r, 24));
            v_data.s_dist_06.assign(RandomStr(r, 24));
            v_data.s_dist_07.assign(RandomStr(r, 24));
            v_data.s_dist_08.assign(RandomStr(r, 24));
            v_data.s_dist_09.assign(RandomStr(r, 24));
            v_data.s_dist_10.assign(RandomStr(r, 24));


            v->s_dist_01.assign(RandomStr(r, 24));
            v->s_dist_02.assign(RandomStr(r, 24));
            v->s_dist_03.assign(RandomStr(r, 24));
            v->s_dist_04.assign(RandomStr(r, 24));
            v->s_dist_05.assign(RandomStr(r, 24));
            v->s_dist_06.assign(RandomStr(r, 24));
            v->s_dist_07.assign(RandomStr(r, 24));
            v->s_dist_08.assign(RandomStr(r, 24));
            v->s_dist_09.assign(RandomStr(r, 24));
            v->s_dist_10.assign(RandomStr(r, 24));
          */
          const stock::key k { (uint64_t) makeStockKey(w, i) };
          checker::SanityCheckStock(&k, v);
          const size_t sz = sizeof(*v);
          stock_total_sz += sz;
          n_stocks++;
          store_->Put(STOC, key, (uint64_t *)wrapper);
        }
        b++;
      } catch (...) {
        printf("[WARNING] stock loader loading abort");
      }
    }
  }

  printf("[CH] finished loading stock\n"
         "[CH]   * average stock record length: %lf bytes\n",
         double(stock_total_sz) / double(n_stocks));

}

void ChStockLoader::loadBackup() {
  uint64_t stock_total_sz = 0, n_stocks = 0;
  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    const size_t batchsize =  NumItems() ;
    const size_t nbatches = (batchsize > NumItems()) ?
                                1 : (NumItems() / batchsize);
    for (uint b = 0; b < nbatches; b++) {
      const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
      for (uint i = (b * batchsize + 1); i <= iend; i++) {
        uint64_t key = makeStockKey(w, i);

        stock::value val;
        stock::value *v = &val;
        v->s_quantity = RandomNumber(rand_, 10, 100);
        v->s_ytd = 0;
        v->s_order_cnt = 0;
        v->s_remote_cnt = 0;

        const int len = RandomNumber(rand_, 26, 50);
        if (RandomNumber(rand_, 1, 100) > 10) {
          const string s_data = RandomStr(rand_, len);
        } else {
          const int startOriginal =
              RandomNumber(rand_, 2, (len - 8));
          const string s_data =
              RandomStr(rand_, startOriginal + 1)
              + "ORIGINAL"
              + RandomStr(rand_, len - startOriginal - 7);
        }
        /*
          v_data.s_dist_01.assign(RandomStr(r, 24));
          v_data.s_dist_02.assign(RandomStr(r, 24));
          v_data.s_dist_03.assign(RandomStr(r, 24));
          v_data.s_dist_04.assign(RandomStr(r, 24));
          v_data.s_dist_05.assign(RandomStr(r, 24));
          v_data.s_dist_06.assign(RandomStr(r, 24));
          v_data.s_dist_07.assign(RandomStr(r, 24));
          v_data.s_dist_08.assign(RandomStr(r, 24));
          v_data.s_dist_09.assign(RandomStr(r, 24));
          v_data.s_dist_10.assign(RandomStr(r, 24));


          v->s_dist_01.assign(RandomStr(r, 24));
          v->s_dist_02.assign(RandomStr(r, 24));
          v->s_dist_03.assign(RandomStr(r, 24));
          v->s_dist_04.assign(RandomStr(r, 24));
          v->s_dist_05.assign(RandomStr(r, 24));
          v->s_dist_06.assign(RandomStr(r, 24));
          v->s_dist_07.assign(RandomStr(r, 24));
          v->s_dist_08.assign(RandomStr(r, 24));
          v->s_dist_09.assign(RandomStr(r, 24));
          v->s_dist_10.assign(RandomStr(r, 24));
        */
        const size_t sz = sizeof(*v);
        stock_total_sz += sz;
        n_stocks++;
        backup_store_->Insert(STOC, key, (char *) v);
      }
    }
  }

  printf("[CH] finished loading stock\n"
         "[CH]   * average stock record length: %lf bytes\n",
         double(stock_total_sz) / double(n_stocks));

}

void ChItemLoader::load() {
  uint64_t total_sz = 0;
  try {
    for (uint i = 1; i <= NumItems(); i++) {
      const item::key k { i };

      int i_size = store_->_schemas[ITEM].total_len;
      char *wrapper = (char *)malloc(i_size);
      memset(wrapper, 0, META_LENGTH);
      item::value *v = (item::value *)(wrapper + META_LENGTH);;
      const string i_name =
        RandomStr(rand_, RandomNumber(rand_, 14, 24));
      v->i_name.assign(i_name);
      v->i_price = (float) RandomNumber(rand_, 100, 10000) / 100.0;
      const int len = RandomNumber(rand_, 26, 50);
      if (RandomNumber(rand_, 1, 100) > 10) {
        const string i_data = RandomStr(rand_, len);
        v->i_data.assign(i_data);
      } else {
        const int startOriginal = RandomNumber(rand_, 2, (len - 8));
        const string i_data = RandomStr(rand_, startOriginal + 1) +
          "ORIGINAL" + RandomStr(rand_, len - startOriginal - 7);
        v->i_data.assign(i_data);
      }
      v->i_im_id = RandomNumber(rand_, 1, 10000);
      checker::SanityCheckItem(&k, v);
      const size_t sz = sizeof(*v);
      total_sz += sz;

      store_->Put(ITEM, i, (uint64_t *)wrapper);
    }
  } catch (...) {
    ALWAYS_ASSERT(false);
  }

  printf("[CH] finished loading item\n"
         "[CH]   * average item record length: %lf bytes\n",
         double(total_sz) / double(NumItems()));
}

void ChItemLoader::loadBackup() {
  uint64_t total_sz = 0;
  for (uint i = 1; i <= NumItems(); i++) {
    item::value val;
    item::value *v = &val;
    const string i_name =
      RandomStr(rand_, RandomNumber(rand_, 14, 24));
    v->i_name.assign(i_name);
    v->i_price = (float) RandomNumber(rand_, 100, 10000) / 100.0;
    const int len = RandomNumber(rand_, 26, 50);
    if (RandomNumber(rand_, 1, 100) > 10) {
      const string i_data = RandomStr(rand_, len);
      v->i_data.assign(i_data);
    } else {
      const int startOriginal = RandomNumber(rand_, 2, (len - 8));
      const string i_data = RandomStr(rand_, startOriginal + 1) +
        "ORIGINAL" + RandomStr(rand_, len - startOriginal - 7);
      v->i_data.assign(i_data);
    }
    v->i_im_id = RandomNumber(rand_, 1, 10000);
    const size_t sz = sizeof(*v);
    total_sz += sz;

    backup_store_->Insert(ITEM, i, (char *) v);
  }

  printf("[CH] finished loading item\n"
         "[CH]   * average item record length: %lf bytes\n",
         double(total_sz) / double(NumItems()));
}

void ChSupplierLoader::load() { assert(false); }

void ChSupplierLoader::loadBackup() {
  uint64_t total_sz = 0;

  for (uint i = 1; i <= NumSupplier(); i++) {
    supplier::value val;
    supplier::value *v = &val;

    int nk = 0;
    do {
      nk = RandomNumber(rand_, '0', 'z');
    } while (nk == 0 || (nk>'9' && nk<'A') || (nk>'Z' && nk<'a'));
    v->su_nationkey = nk;
    v->su_acctbal = RandomNumber(rand_, -99999, 999999) / 100.0;
    const string su_address =
      RandomStr(rand_, RandomNumber(rand_, 10, 40));
    v->su_address.assign(su_address);

    char buf[20];
    sprintf(buf, "Supplier#%09d", i);
    v->su_name.assign(buf);
    sprintf(buf, "%02d-%03d-%03d-%04d", i % 90 + 10,
          RandomNumber(rand_, 100, 999),
          RandomNumber(rand_, 100, 999),
          RandomNumber(rand_, 1000, 9999));
    v->su_phone.assign(buf);

    const string su_comment =
        RandomStr(rand_, RandomNumber(rand_, 25, 100));
    v->su_comment.assign(su_comment);

    const size_t sz = sizeof(*v);
    total_sz += sz;
    backup_store_->Insert(SUPP, i, (char *) v);
  }

  printf("[CH] finished loading supplier\n"
         "[CH]   * average supplier record length: %lf bytes\n",
         double(total_sz) / double(NumSupplier()));

}

ChNationLoader::Nation ChNationLoader::nationNames[] = {
  {48, "ALGERIA", 0}, {49, "ARGENTINA", 1}, {50, "BRAZIL", 1},
  {51, "CANADA", 1}, {52, "EGYPT", 4}, {53, "ETHIOPIA", 0},
  {54, "FRANCE", 3}, {55, "GERMANY", 3}, {56, "INDIA", 2}, {57, "INDONESIA", 2},
  {65, "IRAN", 4}, {66, "IRAQ", 4}, {67, "JAPAN", 2}, {68, "JORDAN", 4},
  {69, "KENYA", 0}, {70, "MOROCCO", 0}, {71, "MOZAMBIQUE", 0}, {72, "PERU", 1},
  {73, "CHINA", 2}, {74, "ROMANIA", 3}, {75, "SAUDI ARABIA", 4},
  {76, "VIETNAM", 2}, {77, "RUSSIA", 3}, {78, "UNITED KINGDOM", 3},
  {79, "UNITED STATES", 1}, {80, "CHINA", 2}, {81, "PAKISTAN", 2},
  {82, "BANGLADESH", 2}, {83, "MEXICO", 1}, {84, "PHILIPPINES", 2},
  {85, "THAILAND", 2}, {86, "ITALY", 3}, {87, "SOUTH AFRICA", 0},
  {88, "SOUTH KOREA", 2}, {89, "COLOMBIA", 1}, {90, "SPAIN", 3},
  {97, "UKRAINE", 3}, {98, "POLAND", 3}, {99, "SUDAN", 0},
  {100, "UZBEKISTAN", 2}, {101, "MALAYSIA", 2}, {102, "VENEZUELA", 1},
  {103, "NEPAL", 2}, {104, "AFGHANISTAN", 2}, {105, "NORTH KOREA", 2},
  {106, "TAIWAN", 2}, {107, "GHANA", 0}, {108, "IVORY COAST", 0},
  {109, "SYRIA", 4}, {110, "MADAGASCAR", 0}, {111, "CAMEROON", 0},
  {112, "SRI LANKA", 2}, {113, "ROMANIA", 3}, {114, "NETHERLANDS", 3},
  {115, "CAMBODIA", 2}, {116, "BELGIUM", 3}, {117, "GREECE", 3},
  {118, "PORTUGAL", 3}, {119, "ISRAEL", 4}, {120, "FINLAND", 3},
  {121, "SINGAPORE", 2}, {122, "NORWAY", 3}
};

void ChNationLoader::load() { assert(false); }

void ChNationLoader::loadBackup() {
  uint64_t total_sz = 0;

  for (uint i = 1; i <= NumNation(); i++) {
    Nation n = nationNames[i - 1];
    nation::value v;

    v.n_regionkey = n.regionId + 1;
    v.n_name.assign(n.name);

    const string n_comment =
        RandomStr(rand_, RandomNumber(rand_, 31, 114));
    v.n_comment.assign(n_comment);

    const size_t sz = sizeof(v);
    total_sz += sz;
    backup_store_->Insert(NATI, n.id, (char *) &v);
  }

  printf("[CH] finished loading nation\n"
         "[CH]   * average nation record length: %lf bytes\n",
         double(total_sz) / double(NumNation()));

}

const char* ChRegionLoader::regionNames[] = {"AFRICA","AMERICA","ASIA","EUROPE", "MIDDLE EAST"};

void ChRegionLoader::load() { assert(false); }

void ChRegionLoader::loadBackup() {
  uint64_t total_sz = 0;
  for (uint i = 1; i <= NumRegion(); i++) {
    region::value v;
    v.r_name.assign(regionNames[i - 1]);
    const string r_comment =
        RandomStr(rand_, RandomNumber(rand_, 31, 115));
    v.r_comment.assign(r_comment);

    const size_t sz = sizeof(v);
    total_sz += sz;
    backup_store_->Insert(REGI, i, (char *) &v);
  }

  printf("[CH] finished loading region\n"
         "[CH]   * average region record length: %lf bytes\n",
         double(total_sz) / double(NumRegion()));

}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc
