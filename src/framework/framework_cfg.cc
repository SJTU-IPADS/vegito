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

#include "framework_cfg.h"

#include <getopt.h>
#include <fstream>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;

namespace nocc {
namespace framework {

Config config;

void Config::parse_sys_args(int argc, char **argv) { 
  exe_name_ = std::string(argv[0] + 2);

  static struct option long_options[] =
    {
      {"verbose"          , no_argument       , 0, 'v'},
      {"bench"            , required_argument , 0, 'b'},
      {"config"           , required_argument , 0, 'k'},
      {"bench-opts"       , required_argument , 0, 'o'},
      {"scale-factor"     , required_argument , 0, 's'},
      {"dist-ratio"       , required_argument , 0, 'r'},
      {"total-partitions" , required_argument , 0, 'T'},
      {"txn-nthreads"     , required_argument , 0, 't'},
      {"query-nthreads"   , required_argument , 0, 'q'},
      {"backup-nthreads"  , required_argument , 0, 'B'},
      {"backup-ratio"     , required_argument , 0, 'R'},
      {"routines"         , required_argument , 0, 'c'},
      {"total-server "    , required_argument , 0, 'm'},
      {"server-id"        , required_argument , 0, 'i'},
      {"fly"              , required_argument , 0, 'f'},
      {"q_fly"            , required_argument , 0, 'F'},
      {0, 0, 0, 0}
    };
  const char *optstring = "vb:k:o:s:r:t:q:c:m:i:B:R:f:F:";

  while (1) {    
    int option_index = 0;
    int opt = getopt_long(argc, argv, optstring, long_options, &option_index);
    if (opt == -1) break;

    switch (opt) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
    case 'v':
      verbose_ = true;
      break;
    case 'b':
      bench_type_ = optarg;
      break;
    case 'k':
      config_file_ = string(optarg);
      break;
    case 'o':
      bench_opts_ = optarg;
      break;
    case 's':
      scale_factor_ = strtoul(optarg, NULL, 10);
      break;
    case 'r':
      distributed_ratio_ = strtoul(optarg, NULL, 10);
      break;
    case 'T':
      assert(false);  // config it in xml
      num_primaries_ = strtoul(optarg, NULL, 10);
      break;
    case 't':
      txn_nthreads_ = strtoul(optarg, NULL, 10);
      break;
    case 'q':
      query_nthreads_ = strtoul(optarg, NULL, 10);
      break;
    case 'B':
      backup_nthreads_ = strtoul(optarg, NULL, 10);
      break;
    case 'R':
      rep_tp_factor_ = strtoul(optarg, NULL, 10);
      break;
    case 'c':
      num_routines_ = strtoul(optarg, NULL, 10);
      break;
    case 'm':
      assert(false);  // config it in xml
      num_servers_ = strtoul(optarg, NULL, 10);
      break;
    case 'i':    
      server_id_ = strtoul(optarg, NULL, 10);
      break;
    case 'f':
      on_fly_ = strtoul(optarg, NULL, 10);
      break;
    case 'F':
      q_on_fly_ = strtoul(optarg, NULL, 10);
      break;

    case '?':
      /* getopt_long already printed an error message. */
    default:
      fprintf(stdout,"Invalid command line val: %s\n", argv[optind - 1]);
      abort();
    }
  }
  optind = 1;  // important: reset optind

  if (num_primaries_ == -1) 
    num_primaries_ = num_servers_;
  assert(num_primaries_ <= num_servers_); 
}

void Config::parse_sys_xml() {
  // test configuration file
  if (config_file_ == "") {
    printf("[System Config] No configure xml file\n");
    return;
  } else {
    ifstream ifs;
    ifs.open(config_file_);
    if (!ifs) {
      printf("[System Config] Cannot open configure file: %s\n", 
             config_file_.c_str());
      return;
    } 
  }
  printf("[System Config] Use configure file: %s\n", config_file_.c_str());
  
  // parse
  using namespace boost::property_tree;
  ptree pt;
  try {
    read_xml(config_file_, pt);
    num_servers_ = pt.get<int>("bench.servers.num");
    assert(num_servers_ >= 1);
  } catch (const ptree_error &e) {
  
  }

  int maci = 0;
  BOOST_FOREACH(ptree::value_type &v, pt.get_child("bench.servers.mapping")) {
    if(maci >= num_servers_) break;
    string s = v.second.data();
    boost::algorithm::trim_right(s);
    boost::algorithm::trim_left(s);
    server_hosts_.push_back(s);
    maci++;
  }
  assert(maci == num_servers_);

  try {
    q_verbose_ = pt.get<int>("bench.query_verbose");
  } catch (const ptree_error &e) {
    q_verbose_ = false;
  }

  try{
    num_primaries_ = pt.get<int>("bench.topo.num_shards");
    assert(num_primaries_ <= num_servers_); 
  } catch (const ptree_error &e) {
    num_primaries_ = num_servers_;
  }
  
  try {
    distributed_ratio_ = pt.get<int>("bench.dist_r");
  } catch (const ptree_error &e) {
    distributed_ratio_ = 1;
  }

  try {
    rep_tp_factor_ = pt.get<int>("bench.rep_tp_factor");
  } catch (const ptree_error &e) {
    
  }

  try {
    rep_ap_factor_ = pt.get<int>("bench.rep_ap_factor");
  } catch (const ptree_error &e) {
    
  }
  try {
    txn_nthreads_ = pt.get<int>("bench.txn_threads");
  } catch (const ptree_error &e) {
  
  }

  try {
    backup_nthreads_ = pt.get<int>("bench.backup_threads");
  } catch (const ptree_error &e) {
    
  }
  if (backup_nthreads_ > txn_nthreads_)
    backup_nthreads_ = txn_nthreads_;

  try {
    query_nthreads_ = pt.get<size_t>("bench.query_threads");
  } catch (const ptree_error &e) {
    
  }
   
  try {
    int sync_ms = pt.get<int>("bench.sync_ms");
    sync_seconds_ = sync_ms / 1000.0;
  } catch (const ptree_error &e) {
    
  } 
  
  try {
    scale_factor_ = pt.get<int>("bench.scale_factor");
  } catch (const ptree_error &e) {
    scale_factor_ = txn_nthreads_;
  }  
  
  try {
    qry_session_ = pt.get<int>("bench.query_session");
  } catch (const ptree_error &e) {
    qry_session_ = 1;
  }  

  try {
    use_logger_ = pt.get<int>("bench.logger");
  } catch (const ptree_error &e) {
    use_logger_ = 0;
  }
  
  try {
    clean_log_ = pt.get<int>("bench.clean_log");
  } catch (const ptree_error &e) {
    clean_log_ = 1;
  }

  try {
    use_epoch_ = pt.get<int>("bench.epoch");
  } catch (const ptree_error &e) {
    use_epoch_ = 0;
  }

  try {
    epoch_type_ = pt.get<int>("bench.epoch_type");
  } catch (const ptree_error &e) {
    epoch_type_ = 0;
  }

  try {
    backup_store_type_ = pt.get<int>("bench.bstore_type");
  } catch (const ptree_error &e) {
    backup_store_type_ = 0;
  }

  try {
    col_split_type_ = pt.get<int>("bench.col_split");
  } catch (const ptree_error &e) {
    col_split_type_ = 0;
  }

  try {
    lazy_index_ = pt.get<bool>("bench.lazy_index");
  } catch (const ptree_error &e) {
    lazy_index_ = false;
  }
  
  try {
    use_index_ = pt.get<bool>("bench.use_index");
  } catch (const ptree_error &e) {
    use_index_ = false;
  }

  // ******** Txn clients ***********
  if (on_fly_ == -1) {
    try {
      use_client_ = pt.get<bool>("bench.client");
    } catch (const ptree_error &e) {
      use_client_ = false;
    }

    try {
      on_fly_ = pt.get<int>("bench.on_fly");   
    } catch (const ptree_error &e) {
      on_fly_ = 1;
    }
  } else {
    use_client_ = true;
  }

  try {
    send_rate_ = pt.get<int>("bench.send_rate");
  } catch (const ptree_error &e) {
    send_rate_ = 1;
  }
  
  // ******** Query clients ***********
  if (q_on_fly_ == -1) {
    try {
      q_use_client_ = pt.get<bool>("bench.q_client");
    } catch (const ptree_error &e) {
      q_use_client_ = false;
    }

    try {
      q_on_fly_ = pt.get<int>("bench.q_on_fly");   
    } catch (const ptree_error &e) {
      q_on_fly_ = 1;
    }
  } else {
    q_use_client_ = true;
  }

  try {
    q_send_rate_ = pt.get<int>("bench.q_send_rate");
  } catch (const ptree_error &e) {
    q_send_rate_ = 1;
  }

  try {
    run_sec_ = pt.get<int>("bench.run_sec");
  } catch (const ptree_error &e) {
    run_sec_ = 40;
  }

  try {
    txn_end_sec_ = pt.get<int>("bench.txn_end_sec"); 
  } catch (const ptree_error &e) {
    txn_end_sec_ = 100;
  }

  try {
    q_start_sec_ = pt.get<int>("bench.q_start_sec"); 
  } catch (const ptree_error &e) {
    q_start_sec_ = 0;
  }

  try {
    q_round_ = pt.get<int>("bench.q_round"); 
  } catch (const ptree_error &e) {
    q_round_ = 100;
  }

  try {
    cleaner_start_sec_ = pt.get<int>("bench.cleaner_start_sec"); 
  } catch (const ptree_error &e) {
    cleaner_start_sec_ = 0;
  }
}

void Config::printConfig() const {
  printf("System Configuration: \n");
  printf("  1. Environments\n");
  printf("    1) exe name: %s\n", exe_name_.c_str());
  printf("    2) config file: %s\n", config_file_.c_str());
  printf("    3) verbose: %d\n", verbose_);
  printf("    4) query verbose: %d\n", q_verbose_);
  printf("  2. Benchmark\n");
  printf("    1) type: %s\n", bench_type_.c_str());
  printf("    2) options: %s\n", bench_opts_.c_str());
  printf("    3) scale factor *of each partition*: %d\n", scale_factor_);
  printf("    4) number of query session: %d\n", qry_session_);
  printf("    5) distributed ration: %d%%\n", distributed_ratio_); 
  printf("  3. Data partition\n");
  printf("    1) #primary partitions: %d\n", num_primaries_);
  printf("    2) backup/TP factor: %d\n", rep_tp_factor_);
  printf("    2) backup/AP factor: %d\n", rep_ap_factor_);
  printf("  4. Server\n");
  printf("    1) #servers: %d\n", num_servers_);
  printf("    2) host name: ");
  for (int i = 0; i < server_hosts_.size(); ++i)
    printf("%s ", server_hosts_[i].c_str());
  printf("\n");
  printf("    3) my server id: %d\n", server_id_);
  printf("  5. Thread model\n");
  printf("    1) #txn threads: %d\n", txn_nthreads_);
  printf("    2) #query threads: %d\n", query_nthreads_);
  printf("    3) #backup threads: %d\n", backup_nthreads_);
  printf("    4) #slave corroutines in each thread: %d\n", num_routines_);
  printf("  6. Log\n");
  printf("    1) use logger: %d\n", use_logger_);
  printf("    2) clean log: %d\n", clean_log_);
  printf("  7. Epoch\n");
  printf("    1) has epoch?: %d\n", use_epoch_);
  printf("    2) epoch type: %d\n", epoch_type_);
  printf("    3) epoch time: %lf seconds\n", sync_seconds_);
  printf("  8. Backup Store\n");
  printf("    1) backup store type: %d\n", backup_store_type_);
  printf("    2) column split type: %d\n", col_split_type_);
  printf("    3) use index?: %d\n", use_index_);
  printf("    4) lazy index?: %d\n", lazy_index_);
  printf("  8. Client\n");
  printf("    1) has client?: %d\n", use_client_);
  printf("    2) on fly request: %d\n", on_fly_);
  printf("    3) send rate: %d\n", send_rate_);
  printf("    4) has query client?: %d\n", q_use_client_);
  printf("    5) q on fly request: %d\n", q_on_fly_);
  printf("    6) q send rate: %d\n", q_send_rate_);
  printf("  9. Time\n");
  printf("    1) run time sec: %d\n", run_sec_);
  printf("    2) txn end sec: %d\n", txn_end_sec_);
  printf("    3) query start sec: %d\n", q_start_sec_);
  printf("    4) query round: %d\n", q_round_);
  printf("    5) cleaner start sec: %d\n", cleaner_start_sec_);
 
  printf(" 10. Macro\n");
  printf("    1) use backup store: %d\n", use_backup_store_);
}

} // namespace framework
} // namespace nocc
