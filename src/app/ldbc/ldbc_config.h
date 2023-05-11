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

#pragma once

#include <cassert>
#include <vector>
#include <string>

#include "framework/framework_cfg.h"

namespace nocc {
namespace oltp {
namespace ldbc {

class LDBCConfig {
 public:
  void parse_ldbc_args(int argc, char **argv);
  void parse_ldbc_xml(const std::string &xml);
  void printConfig() const;

  inline const std::string &getRoot() const {
    return g_root_;
  }

  inline const std::vector<std::string> &getQueryWorkload() const { 
    return g_query_workload_; 
  };

  inline const std::vector<std::string> &getAnalyticsWorkload() const { 
    return g_analytics_workload_; 
  };

  void fill_filename() const;

 private:
  std::string g_root_;  // root dir name of LDBC data
  std::vector<std::string> g_query_workload_;  // id of queries
  std::vector<std::string> g_analytics_workload_;  // id of analytics
};

extern LDBCConfig ldbcConfig;

// static vertex
extern std::string ORGANISATION_FILE;
extern std::string PLACE_FILE;
extern std::string TAG_FILE;
extern std::string TAGCLASS_FILE;

// dynamic vertex
extern std::string PERSON_FILE;
extern std::string COMMENT_FILE;
extern std::string POST_FILE;
extern std::string FORUM_FILE;

// static edge (all simple edge)
extern std::string ORG_ISLOCATIONIN_FILE;
extern std::string ISPARTOF_FILE;
extern std::string ISSUBCLASSOF_FILE;
extern std::string HASTYPE_FILE;

// dynamic edge (simple edge)
extern std::string COMMENT_HASCREATOR_FILE;
extern std::string COMMENT_HASTAG_FILE;
extern std::string COMMENT_ISLOCATIONIN_FILE;
extern std::string REPLYOF_COMMENT_FILE;
extern std::string REPLYOF_POST_FILE;
  
extern std::string POST_HASCREATOR_FILE;
extern std::string POST_HASTAG_FILE;
extern std::string POST_ISLOCATIONIN_FILE;

extern std::string FORUM_CONTAINEROF_FILE;
extern std::string FORUM_HASMODERATOR_FILE;
extern std::string FORUM_HASTAG_FILE;

extern std::string PERSON_HASINTEREST_FILE;
extern std::string PERSON_ISLOCATEDIN_FILE;

// dynamic edge (property edge)
extern std::string FORUM_HASMEMBER_FILE;
extern std::string KNOWS_FILE;
extern std::string LIKES_COMMENT_FILE;
extern std::string LIKES_POST_FILE;
extern std::string STUDYAT_FILE;
extern std::string WORKAT_FILE;

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc
