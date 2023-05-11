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

#include "ch_config.h"
#include "framework/utils/macros.h"
#include <getopt.h>

// for parsing xml
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

using namespace std;

static inline vector<string>
  split(const string &s, char delim)
{
  vector<string> elems;
  stringstream ss(s);
  string item;
  while (getline(ss, item, delim))
    elems.emplace_back(item);
  return elems;
}

static inline std::string trim(std::string str)
{
    // remove trailing white space
    while(!str.empty() && std::isspace(str.back())) str.pop_back();

    // return residue after leading white space
    std::size_t pos = 0;
    while(pos < str.size() && std::isspace(str[pos])) ++pos;
    return str.substr(pos);
}

namespace nocc {
namespace oltp {
namespace ch {

ChConfig chConfig;

void ChConfig::parse_ch_args(int argc, char **argv) {
  static struct option long_options[] =
    {
      {"uniform-item-dist"        , no_argument       , 0, 'u'},
      {"workload-mix"             , required_argument , 0, 'w'},
      {"query-id"                 , required_argument , 0, 'q'},
      {0, 0, 0, 0}
    };
  
  while (1) {  
    int option_index = 0;
    int opt = getopt_long(argc, argv, "uw:q:", long_options, &option_index);
    if (opt == -1) break;

    switch (opt) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
    case 'u':
      g_uniform_item_dist_ = true;
      break;
    case 'w':
      {
        // assert(false);
        vector<string> toks = split(optarg, ',');
        ALWAYS_ASSERT(toks.size() == ChConfig::TXN_NUM);
        int total = 0;
        for (int i = 0; i < ChConfig::TXN_NUM; ++i) {
          int p = strtoul(toks[i].c_str(), NULL, 10);
          assert(p >= 0 && p <= 100);
          g_txn_workload_mix_[i] = p;
          total += p;
        }
        assert(total == 100);
      }
      break;
    case 'q':
      {
        vector<string> qid_str = split(optarg, ',');
        for (int i = 0; i < qid_str.size(); ++i) {
          int query_id = strtoul(qid_str[i].c_str(), NULL, 10);
          g_query_workload_.push_back(query_id);
        }
      }
      break;
    case '?':
      /* getopt_long already printed an error message. */
    default:
      fprintf(stdout,"Invalid command line val: %s\n", argv[optind - 1]);
      abort();
    }
  }
}

void ChConfig::parse_ch_xml(const std::string &config_file) {
  // test configuration file
  if (config_file == "") {
    printf("[CH Config] No configure xml file\n");
    return;
  } else {
    ifstream ifs;
    ifs.open(config_file);
    if (!ifs) {
      printf("[CH Config] Cannot open configure file: %s\n", 
             config_file.c_str());
      return;
    } 
  }
  printf("[CH Config] Use configure file: %s\n", config_file.c_str());

  // parse input xml
  using namespace boost::property_tree;
  ptree pt;

  try {
    read_xml(config_file, pt);

    // txn workload
    int txn_wordload_mix[] = { 
      pt.get<int> ("bench.ch.new"),
      pt.get<int> ("bench.ch.pay"),
      pt.get<int> ("bench.ch.del"),
      pt.get<int> ("bench.ch.stock"),
      pt.get<int> ("bench.ch.order")
    };

    int total = 0;
    for (int i = 0; i < ChConfig::TXN_NUM; ++i) {
      int p = txn_wordload_mix[i];
      assert(p >= 0 && p <= 100);
      g_txn_workload_mix_[i] = p;
      total += p;
    }
    assert(total == 100);

    // query workload
    string str = pt.get<string> ("bench.ch.query");
    vector<string> qid_str = split(str, ',');
    g_query_workload_.clear();
    for (int i = 0; i < qid_str.size(); ++i) {
      int query_id = strtoul(qid_str[i].c_str(), NULL, 10);
      g_query_workload_.push_back(query_id);
    }

    // analytics workload
    string a_workload_str = pt.get<string> ("bench.ch.analytics");
    vector<string> algo_strs = split(a_workload_str, ',');
    g_analytics_workload_.clear();
    for (int i = 0; i < algo_strs.size(); ++i) {
      g_analytics_workload_.push_back(trim(algo_strs[i]));
    }

    // analytics params
    string a_params_str = pt.get<string> ("bench.ch.analytics_param");
    vector<string> params_strs = split(a_params_str, ',');
    g_analytics_params_.clear();
    for (int i = 0; i < params_strs.size(); ++i) {
      g_analytics_params_.push_back(trim(params_strs[i]));
    }
  } catch (const ptree_error &e) {
  
  }
}

void ChConfig::printConfig() const {
  printf("Ch Benchmark Configuration: \n");
  printf("  1. uniform item dist: %d\n", g_uniform_item_dist_);
  printf("  2. transactionl workload:\n");
  printf("    1) New order: %d%%\n", g_txn_workload_mix_[NEW_ORDER]);
  printf("    2) Payment: %d%%\n", g_txn_workload_mix_[PAYMENT]);
  printf("    3) Delivery: %d%%\n", g_txn_workload_mix_[DELIVERY]);
  printf("    4) Order status: %d%%\n", g_txn_workload_mix_[ORDER_STATUS]);
  printf("    5) Stock level: %d%%\n", g_txn_workload_mix_[STOCK_LEVEL]);
  printf("  3. query workload: ");
  for (int i = 0; i < g_query_workload_.size(); ++i) {
    printf("Q%d ", g_query_workload_[i]);
  }
  printf("\n");
  printf("  4. analytics workload: ");
  for (int i = 0; i < g_analytics_workload_.size(); ++i) {
    printf("%s ", g_analytics_workload_[i].c_str());
  }
  printf("\n");
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc

