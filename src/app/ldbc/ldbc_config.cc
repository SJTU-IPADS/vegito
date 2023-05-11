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

#include "ldbc_config.h"
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
namespace ldbc {

LDBCConfig ldbcConfig;

// static vertex
string ORGANISATION_FILE;
string PLACE_FILE;
string TAG_FILE;
string TAGCLASS_FILE;

// dynamic vertex
string PERSON_FILE;
string COMMENT_FILE;
string POST_FILE;
string FORUM_FILE;
  
// static edge (all simple edge)
string ORG_ISLOCATIONIN_FILE;
string ISPARTOF_FILE;
string ISSUBCLASSOF_FILE;
string HASTYPE_FILE;

// dynamic edge (simple edge)
string COMMENT_HASCREATOR_FILE;
string COMMENT_HASTAG_FILE;
string COMMENT_ISLOCATIONIN_FILE;
string REPLYOF_COMMENT_FILE;
string REPLYOF_POST_FILE;
  
string POST_HASCREATOR_FILE;
string POST_HASTAG_FILE;
string POST_ISLOCATIONIN_FILE;

string FORUM_CONTAINEROF_FILE;
string FORUM_HASMODERATOR_FILE;
string FORUM_HASTAG_FILE;

string PERSON_HASINTEREST_FILE;
string PERSON_ISLOCATEDIN_FILE;

// dynamic edge (property edge)
string FORUM_HASMEMBER_FILE;
string KNOWS_FILE;
string LIKES_COMMENT_FILE;
string LIKES_POST_FILE;
string STUDYAT_FILE;
string WORKAT_FILE;

void LDBCConfig::parse_ldbc_args(int argc, char **argv) {
  static struct option long_options[] =
    {
      {"ldbc-root"            , required_argument , 0, 'd'},
      {"analytics-id"         , required_argument , 0, 'a'},
      {0, 0, 0, 0}
    };
  
  while (1) {  
    int option_index = 0;
    int opt = getopt_long(argc, argv, "d:a:", long_options, &option_index);
    if (opt == -1) break;

    switch (opt) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
    case 'd':
      g_root_ = trim(optarg);
      break;
    case 'q':
      {
        vector<string> algo_strs = split(optarg, ',');
        g_analytics_workload_.clear();
        for (int i = 0; i < algo_strs.size(); ++i) {
          g_analytics_workload_.push_back(trim(algo_strs[i]));
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

void LDBCConfig::parse_ldbc_xml(const std::string &config_file) {
  // test configuration file
  if (config_file == "") {
    printf("[LDBC Config] No configure xml file\n");
    return;
  } else {
    ifstream ifs;
    ifs.open(config_file);
    if (!ifs) {
      printf("[LDBC Config] Cannot open configure file: %s\n", 
             config_file.c_str());
      return;
    } 
  }
  printf("[LDBC Config] Use configure file: %s\n", config_file.c_str());

  // parse input xml
  using namespace boost::property_tree;
  ptree pt;

  try {
    read_xml(config_file, pt);

    g_root_ = trim(pt.get<string> ("bench.ldbc.ldbc_root"));

    // query workload
    string str = pt.get<string> ("bench.ldbc.query");
    vector<string> queries_str = split(str, ',');
    g_query_workload_.clear();
    for (int i = 0; i < queries_str.size(); ++i) {
      g_query_workload_.push_back(trim(queries_str[i]));
    }

    // analytics workload
    string a_workload_str = pt.get<string> ("bench.ldbc.analytics");
    vector<string> algo_strs = split(a_workload_str, ',');
    g_analytics_workload_.clear();
    for (int i = 0; i < algo_strs.size(); ++i) {
      g_analytics_workload_.push_back(trim(algo_strs[i]));
    }
  } catch (const ptree_error &e) {
  
  }
}

void LDBCConfig::printConfig() const {
  printf("LDBC Benchmark Configuration: \n");
  printf("  1. LDBC data root: %s\n", g_root_.c_str());
  printf("  2. query workload: ");
  for (int i = 0; i < g_query_workload_.size(); ++i) {
    printf("%s ", g_query_workload_[i].c_str());
  }
  printf("\n");
  printf("  3. analytics workload: ");
  for (int i = 0; i < g_analytics_workload_.size(); ++i) {
    printf("%s ", g_analytics_workload_[i].c_str());
  }
  printf("\n");
}

void LDBCConfig::fill_filename() const {
  string LDBC_DIR = getRoot();
  assert(LDBC_DIR.size() != 0);
  const string LDBC_STATIC = LDBC_DIR + "/static/";
  const string LDBC_DYNAMIC = LDBC_DIR + "/dynamic/";
  
  // static vertex
  ORGANISATION_FILE  = LDBC_STATIC + "organisation_0_0.csv";
  PLACE_FILE         = LDBC_STATIC + "place_0_0.csv";
  TAG_FILE           = LDBC_STATIC + "tag_0_0.csv";
  TAGCLASS_FILE      = LDBC_STATIC + "tagclass_0_0.csv";

  // dynamic vertex
  PERSON_FILE  = LDBC_DYNAMIC + "person_0_0.csv";
  COMMENT_FILE = LDBC_DYNAMIC + "comment_0_0.csv";
  POST_FILE    = LDBC_DYNAMIC + "post_0_0.csv";
  FORUM_FILE   = LDBC_DYNAMIC + "forum_0_0.csv";
  
  // static edge (all simple edge)
  ORG_ISLOCATIONIN_FILE 
    = LDBC_STATIC + "organisation_isLocatedIn_place_0_0.csv";
  ISPARTOF_FILE 
    = LDBC_STATIC + "place_isPartOf_place_0_0.csv";
  ISSUBCLASSOF_FILE 
    = LDBC_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv";
  HASTYPE_FILE 
    = LDBC_STATIC + "tag_hasType_tagclass_0_0.csv";

  // dynamic edge (simple edge)
  COMMENT_HASCREATOR_FILE 
      = LDBC_DYNAMIC + "comment_hasCreator_person_0_0.csv";
  COMMENT_HASTAG_FILE 
      = LDBC_DYNAMIC + "comment_hasTag_tag_0_0.csv";
  COMMENT_ISLOCATIONIN_FILE 
      = LDBC_DYNAMIC + "comment_isLocatedIn_place_0_0.csv";
  REPLYOF_COMMENT_FILE 
      = LDBC_DYNAMIC + "comment_replyOf_comment_0_0.csv";
  REPLYOF_POST_FILE 
      = LDBC_DYNAMIC + "comment_replyOf_post_0_0.csv";
  
  POST_HASCREATOR_FILE 
      = LDBC_DYNAMIC + "post_hasCreator_person_0_0.csv";
  POST_HASTAG_FILE
      = LDBC_DYNAMIC + "post_hasTag_tag_0_0.csv";
  POST_ISLOCATIONIN_FILE
      = LDBC_DYNAMIC + "post_isLocatedIn_place_0_0.csv";

  FORUM_CONTAINEROF_FILE
      = LDBC_DYNAMIC + "forum_containerOf_post_0_0.csv";
  FORUM_HASMODERATOR_FILE
      = LDBC_DYNAMIC + "forum_hasModerator_person_0_0.csv";
  FORUM_HASTAG_FILE
      = LDBC_DYNAMIC + "forum_hasTag_tag_0_0.csv";

  PERSON_HASINTEREST_FILE
      = LDBC_DYNAMIC + "person_hasInterest_tag_0_0.csv";
  PERSON_ISLOCATEDIN_FILE
      = LDBC_DYNAMIC + "person_isLocatedIn_place_0_0.csv";

  // dynamic edge (property edge)
  FORUM_HASMEMBER_FILE
      = LDBC_DYNAMIC + "forum_hasMember_person_0_0.csv";
  KNOWS_FILE   = LDBC_DYNAMIC + "person_knows_person_0_0.csv";
  LIKES_COMMENT_FILE
      = LDBC_DYNAMIC + "person_likes_comment_0_0.csv";
  LIKES_POST_FILE
      = LDBC_DYNAMIC + "person_likes_post_0_0.csv";
  STUDYAT_FILE
      = LDBC_DYNAMIC + "person_studyAt_organisation_0_0.csv";
  WORKAT_FILE
      = LDBC_DYNAMIC + "person_workAt_organisation_0_0.csv";
}

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

