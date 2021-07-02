/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA for speedy distributed
 *  in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */

#ifndef CH_SCHEMA_H_
#define CH_SCHEMA_H_

#include "framework/utils/inline_str.h"
#include "framework/framework_cfg.h"

namespace nocc {
namespace oltp {
namespace ch {

typedef float AMOUNT;

// ************ txn id
const int TID_NEW_ORDER = 0;
const int TID_PAYMENT = 1;
const int TID_DELIVERY = 2;

// ************ table id
const int WARE = 0;
const int DIST = 1;
const int CUST = 2;
const int HIST = 3;
const int NEWO = 4;
const int ORDE = 5;
const int ORLI = 6;
const int ITEM = 7;
const int STOC = 8;
const int CUST_INDEX = 9;
const int ORDER_INDEX = 10;

const int SUPP = 11;
const int NATI = 12;
const int REGI = 13;

// *********** column id

// 0. warehouse
const int W_YTD   = 0;
const int W_TAX   = 1;
const int W_NAME  = 2;
struct warehouse {
  struct PACKED key {
    uint64_t w_id;
  };
  
  struct PACKED value {
    int64_t w_ytd;  // signed numeric(12,2)
    float w_tax;    // signed numeric(4,4)
    inline_str_8<10> w_name;
    inline_str_8<20> w_street_1;
    inline_str_8<20> w_street_2;
    inline_str_8<20> w_city;
    inline_str_fixed<2> w_state;
    inline_str_fixed<9> w_zip;
  }; 
};

// 1. district
const int D_YTD       = 0;
const int D_NEXT_O_ID = 1;
const int D_TAX       = 2;
const int D_NAME      = 3;
struct district {
  struct PACKED key {
    uint64_t d_id;
  };
  
  struct PACKED value {
    int64_t d_ytd;        // signed numeric(12,2))
    int32_t d_next_o_id;  // 10,000,000 unique IDs
    float d_tax;          // signed numeric(4,4)
    inline_str_8<10> d_name;
    inline_str_8<20> d_street_1;
    inline_str_8<20> d_street_2;
    inline_str_8<20> d_city;
    inline_str_fixed<2> d_state;
    inline_str_fixed<9> d_zip;
  }; 
};

// 2. customer
const int C_BALANCE      = 0;
const int C_YTD_PAYMENT  = 1;
const int C_PAYMENT_CNT  = 2;
const int C_DELIVERY_CNT = 3;
const int C_DATA         = 4;
const int C_DISCOUNT     = 5;

const int C_LAST         = 7;

const int C_CITY         = 12;
const int C_STATE        = 13;
const int C_PHONE        = 15;

struct customer {
  struct PACKED key {
    uint64_t c_id;
  };
  
  struct PACKED value {
    AMOUNT c_balance;  // signed numeric(12, 2)
    AMOUNT c_ytd_payment;  // signed numeric(12, 2)
    int32_t c_payment_cnt;  // numeric(4)
    int32_t c_delivery_cnt;  // numeric(4)
    inline_str_16<500> c_data;
    float c_discount;  // signed numeric(4, 4)
    inline_str_fixed<2> c_credit;
    inline_str_8<16> c_last;
    inline_str_8<16> c_first;
    float c_credit_lim;  // signed numeric(12, 2)
    inline_str_8<20> c_street_1;
    inline_str_8<20> c_street_2;
    inline_str_8<20> c_city;
    inline_str_fixed<2> c_state;
    inline_str_fixed<9> c_zip;
    inline_str_fixed<16> c_phone;
    uint32_t c_since;  // date and time
    inline_str_fixed<2> c_middle;
    // uint8_t c_n_nationkey;
  }; 
};

// 3. history
struct history {
  struct PACKED key {
    uint64_t h_id;
  };
  
  struct PACKED value {
    uint32_t h_date;  // date and time
    AMOUNT h_amount;  // signed numeric(6, 2)
    inline_str_8<24> h_data;
  }; 
};

// 4. new order
struct new_order {
  struct PACKED key {
    uint64_t no_id;
  };
  
  struct PACKED value { 
    // inline_str_fixed<12> no_dummy;
  };
};


// 5. order
const int O_CARRIER_ID   = 0;
const int O_C_ID         = 1;
const int O_OL_CNT       = 2;
const int O_ENTRY_D      = 4;
struct oorder {
  struct PACKED key {
    uint64_t o_id;
  };
  
  struct PACKED value {
    int32_t o_carrier_id;  // 10 unique IDs, or null
    int32_t o_c_id;  // 96,000 unique IDs
    int8_t o_ol_cnt;  // numeric(2)
    bool o_all_local;  // numeric(1)
    uint32_t o_entry_d;  // date and time
  }; 
};

// 6. order line
const int OL_DELIVERY_D  = 0;
const int OL_I_ID        = 1;
const int OL_AMOUNT      = 2;
const int OL_SUPPLY_W_ID = 3;
const int OL_QUANTITY    = 4;
struct order_line {
  struct PACKED key {
    uint64_t ol_id;
  };
  
  struct PACKED value {
#if FRESHNESS == 0  // 1 for freshness
    uint32_t ol_delivery_d;  // date and time, or null
#else
    uint64_t ol_delivery_d;  // date and time, or null
#endif
    int32_t ol_i_id;  
    AMOUNT ol_amount;  // signed numeric(6, 2)
    int32_t ol_supply_w_id;
    int8_t ol_quantity;
  }; 
};

// 7. item
const int I_NAME    =  0;
const int I_PRICE   =  1;
const int I_DATA    =  2;
const int I_IM_ID   =  3;
struct item {
  struct PACKED key {
    uint64_t i_id;
  };
  
  struct PACKED value {
    inline_str_8<24> i_name;
    float i_price;  // numeric(5, 2)
    inline_str_8<50> i_data;
    int32_t i_im_id;
  }; 
};

// 8. stock
const int S_QUANTITY   = 0;
const int S_YTD        = 1;
const int S_ORDER_CNT  = 2;
const int S_REMOTE_CNT = 3;

struct stock {
  struct PACKED key {
    uint64_t s_id;
  };
  
  struct PACKED value {
    int16_t s_quantity;  // signed numeric(4)
    AMOUNT s_ytd;  // numeric(8)
    int32_t s_order_cnt;  // numeric(4)
    int32_t s_remote_cnt;  // numeric(4)
  }; 
};

// 9/10. index  XXX: fix it

// 11. supplier
const int SU_NATIONKEY   = 0;
const int SU_ACCTBAL     = 1;
const int SU_PHONE       = 2;
const int SU_NAME        = 3;
const int SU_ADDRESS     = 4;
const int SU_COMMENT     = 5;
struct supplier {
  struct PACKED key {
    uint64_t su_suppkey;
  };
  
  struct PACKED value {
    int8_t su_nationkey;
    float su_acctbal;
    inline_str_fixed<15> su_phone;
    inline_str_fixed<20> su_name;
    inline_str_8<40> su_address;
    inline_str_8<100> su_comment;
  }; 
};

// 12. nation
const int N_REGIONKEY   = 0;
const int N_NAME        = 1;
const int N_COMMONT     = 2;
struct nation {
  struct PACKED key {
    uint64_t n_nationkey;
  };
  
  struct PACKED value {
    int8_t n_regionkey;
    inline_str_8<15> n_name;
    inline_str_8<114> n_comment;
  };
};

// 13. region
const int R_NAME        = 0;
const int R_COMMENT     = 1;
struct region {
  struct PACKED key {
    uint64_t r_regionkey; 
  };

  struct PACKED value {
    inline_str_8<15> r_name;
    inline_str_8<115> r_comment;
  };
};

}  // namespace ch
}  // namespace oltp
}  // namespace noccx

#endif  // CH_SCHEMA_H_
