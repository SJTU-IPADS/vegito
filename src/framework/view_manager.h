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

// This module will initiliaze the views of the test environment,
// by parsing the config file

/*
 *   Format of the configuration file:
 *   N ( how many machine in the cluster)
 *   ip0
 *   ...
 *   ipN
 *   P ( how many partitions for the test)
 *   0 1 ( 0 partition  is on machine 0 with backup @ machine 1)
 */


#ifndef NOCC_FRAMEWORK_VIEW_MANAGER_H_
#define NOCC_FRAMEWORK_VIEW_MANAGER_H_

#include <string>
#include <vector>
#include <deque>
#include <set>

#include <cassert>

namespace nocc {
namespace framework {

// arrage partitions on each machines
// primaries: p_id (0,1,2,...) for global
// backup_id: 0,1,2,... for each primary
// backups: TP backups | AP backups | GP backups
// TP backups: backup_id in [0, tp_factor_)
// AP backups: backup_id in [tp_factor_, tp_factor_ + ap_factor_)
// GP backups: backup_id in 
//            [tp_factor_ + ap_factor_, tp_factor_ + ap_factor_ + gp_factor_)

class View {
 public:
  View() { };

  void init_view();  // must call after parse global config

  // Print current view in a user visable form
  void print_view() const;

  inline int get_backup_pid(int mac_id, int backup_id) const {
   assert(backup_id < rep_factor_);

    int res;
    if (is_tp(backup_id))         // tp_replica
      res = macs_[mac_id].tp_backups[backup_id];
    else if (is_ap(backup_id))    // ap_replica
      res = macs_[mac_id].ap_backups[backup_id - tp_factor_];
    else if (is_gp(backup_id))    // gp_replica
      res = macs_[mac_id].gp_backups[backup_id - tp_factor_ - ap_factor_];
    else {
      printf("error backup_id\n");
      assert(false);
    }

    return res;

  }

  // query the backup shards i am responsible for, return the backup number
  inline const std::vector<int> &get_tp_backups(int mac_id) const {
    return macs_[mac_id].tp_backups;
  }

  // query the backup shards i am responsible for, return the backup number
  inline const std::vector<int> &get_ap_backups(int mac_id) const {
    return macs_[mac_id].ap_backups;
  }
  
  // query the backup shards i am responsible for, return the backup number
  inline const std::vector<int> &get_gp_backups(int mac_id) const {
    return macs_[mac_id].gp_backups;
  }

  // query the primary shards the mac whose id == `mac_id` is responsible for
  // return the number of primary shards
  inline const std::vector<int> &get_primaries(int mac_id) const {
    return macs_[mac_id].primaries;
  }

  inline int get_backup_mac(int p_id, int backup_id) const {
    assert(backup_id < rep_factor_);

    int res;
    if (is_tp(backup_id))         // tp_replica
      res = partitions_[p_id].b_tp_macs[backup_id];
    else if (is_ap(backup_id))    // ap_replica
      res = partitions_[p_id].b_ap_macs[backup_id - tp_factor_];
    else if (is_gp(backup_id))
      res = partitions_[p_id].b_gp_macs[backup_id - tp_factor_ - ap_factor_];
    else {
      printf("error backup_id\n");
      assert(false);
    }

    return res;
  }

  inline bool is_tp(int backup_id) const { return (backup_id < tp_factor_); }
  inline bool is_ap(int backup_id) const { 
    return (backup_id >= tp_factor_ && backup_id < tp_factor_ + ap_factor_); 
  }
  inline bool is_gp(int backup_id) const {
    return (backup_id >= tp_factor_ + ap_factor_);
  }

  // return the primary mac id for a partition
  inline int partition_to_mac(int p_id) const {
    return (p_id % num_mac_);
  }

 private:

  struct MacInfo { 
    std::string host_name;

    std::vector<int> primaries;  // primary partitions
    std::vector<int> tp_backups;  // backup/tp partitions
    std::vector<int> ap_backups;  // backup/ap partitions
    std::vector<int> gp_backups;  // backup/gp partitions

    MacInfo(const std::string &name)
      : host_name(name) { }
  };
  
  // data partition (shard)
  struct PartitionInfo {
    int pmac;  // primary machine
    std::vector<int> b_tp_macs;  // backup/TP, index by backup id (0, 1, ..)
    std::vector<int> b_ap_macs;  // backup/AP
    std::vector<int> b_gp_macs;  // backup/GP

    PartitionInfo(int mac)
      : pmac(mac) { }
  };
  
  void arrage_primary_(int num_partitions);
  void arrange_backup_();

  // query whether a machine is responsible for some job in at a partition
  bool response(int mac_id,int p_id) const;

  std::vector<PartitionInfo> partitions_;  // p_id (0,1,2,...)->PartitionInfo 
  std::vector<MacInfo> macs_;  // mac_id (0,1,2,...)->MacInfo

  int num_mac_;
  int rep_factor_;
  int tp_factor_;
  int ap_factor_;
  int gp_factor_;
};

extern View view;

} // namespace oltp
} // namespace nocc

#endif
