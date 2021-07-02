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

#include "view_manager.h"
#include "framework_cfg.h"

#include <stdio.h>
#include <cassert>

using namespace std;

namespace nocc {
namespace framework {

View view;

void View::init_view() {
  // read config
  int num_partitions = config.getNumPrimaries();
  const vector<string> &hosts = config.getServerHosts();

  for (const string &host : hosts) {
    macs_.emplace_back(host); 
  }
  partitions_.reserve(num_partitions);
  
  // start allocating the primary/backup jobs to machines
  // first allocate primaries, which ensures primaries are at different machines
  num_mac_ = hosts.size();
  tp_factor_ = config.getTPFactor();
  ap_factor_ = config.getAPFactor();

  arrage_primary_(num_partitions);
  arrange_backup_();
}

void View::arrage_primary_(int num_partitions) {
  for(int p_id = 0; p_id < num_partitions; ++p_id) {
    int mac_id = partition_to_mac(p_id);
    partitions_.emplace_back(mac_id);

    MacInfo &mac_info = macs_[mac_id];
    mac_info.primaries.push_back(p_id);
  }
}

void View::arrange_backup_() {
  int rep_num = 1;
  for( ; rep_num <= tp_factor_; ++rep_num) {
    for(int p_id = 0; p_id < partitions_.size(); ++p_id) {

      PartitionInfo& partition = partitions_[p_id];
      int mac_id = (partition.pmac + rep_num) % macs_.size();
      // when the machines is enough
      if (partitions_.size() >= 1 + tp_factor_ + ap_factor_)
        assert(!response(mac_id,p_id));
      
      partition.b_tp_macs.push_back(mac_id);

      MacInfo& mac_info = macs_[mac_id];
      mac_info.tp_backups.push_back(p_id);
    }
  }

  for( ; rep_num <= tp_factor_ + ap_factor_; ++rep_num) {
    for(int p_id = 0; p_id < partitions_.size(); ++p_id) {

      PartitionInfo& partition = partitions_[p_id];
      int mac_id = (partition.pmac + rep_num) % macs_.size();
      if (partitions_.size() >= 1 + tp_factor_ + ap_factor_)
        assert(!response(mac_id,p_id));
      
      partition.b_ap_macs.push_back(mac_id);

      MacInfo& mac_info = macs_[mac_id];
      mac_info.ap_backups.push_back(p_id);
    }
  }

}

void View::print_view() const {

  if (macs_.size() == 0) {
    fprintf(stderr,"The view has not been initilized.\n");
    return;
  }

  printf("[View] All life machine in the cluster: \n");
  for (const MacInfo &mac_info : macs_) {
    printf("%s\n", mac_info.host_name.c_str());

    printf("  #primaries: %lu, #tp_backups: %lu, #ap_backups: %lu ",
            mac_info.primaries.size(), mac_info.tp_backups.size(),
            mac_info.ap_backups.size());

    printf("partitions: ");
    for (int p_id : mac_info.primaries) printf("%d ", p_id);
    printf("| ");
    for (int b_id : mac_info.tp_backups) printf("%d ", b_id);
     printf("| ");
    for (int b_id : mac_info.ap_backups) printf("%d ", b_id);
    
    printf("\n");
  }
  printf("\n");

  printf("There are %lu partitions: \n", partitions_.size());
  for(uint p_id = 0; p_id < partitions_.size(); ++p_id) {
    const PartitionInfo &p = partitions_[p_id];
    printf("P%d at %d: ", p_id, p.pmac);

    if(p.b_tp_macs.size() == 0) {
      printf("no Backup/TP");
    } else {
      printf("TP backed by ");
      for (int backup_mac : p.b_tp_macs) printf("%d ", backup_mac);
    }

    if(p.b_ap_macs.size() == 0) {
      printf("; no Backup/AP");
    } else {
      printf("; AP backed by ");
      for (int backup_mac : p.b_ap_macs) printf("%d ", backup_mac);
    }
    printf("\n"); 
  }
 
  printf("\n"); 
}

bool View::response(int mac_id,int p_id) const {
  const PartitionInfo &partition = partitions_[p_id];
  if(mac_id == partition.pmac)
    return true;

  for (int b_tp_mac : partition.b_tp_macs) {
    if (mac_id == b_tp_mac) return true;
  }

  for (int b_ap_mac : partition.b_ap_macs) {
    if (mac_id == b_ap_mac) return true;
  }

  return false;
}

}  // namespace oltp
}  // namespace nocc
