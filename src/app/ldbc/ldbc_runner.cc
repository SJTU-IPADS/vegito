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

#include "ldbc_runner.h"
#include "ldbc_schema.h"
#include "ldbc_config.h"
#include "ldbc_graph_loader.h"
#include "ldbc_query.h"
#include "ldbc_worker.h"
#include "ldbc_log_cleaner.h"
#include "ldbc_analytics.h"

using namespace std;

namespace nocc {
namespace oltp {
namespace ldbc {

#if FRESHNESS == 0
const int GP_EDGE_LOAD_PCT = 50;  // [0, 100]
#else
const int GP_EDGE_LOAD_PCT = 0;  // [0, 100]
#endif

const int TP_EDGE_LOAD_PCT = 100 - GP_EDGE_LOAD_PCT;  // [0, 100]

LDBCRunner::LDBCRunner() {
  edge_meta_ = {
    // static
    { ORG_ISLOCATIONIN_FILE, ORG_ISLOCATIONIN, ORGANISATION, PLACE },
    { ISPARTOF_FILE, ISPARTOF, PLACE, PLACE },
    { ISSUBCLASSOF_FILE, ISSUBCLASSOF, TAGCLASS, TAGCLASS },
    { HASTYPE_FILE, HASTYPE, TAG, TAGCLASS },

    // dynamic w/o property
    { COMMENT_HASCREATOR_FILE, COMMENT_HASCREATOR, COMMENT, PERSON },
    { COMMENT_HASTAG_FILE, COMMENT_HASTAG, COMMENT, TAG },
    { COMMENT_ISLOCATIONIN_FILE, COMMENT_ISLOCATIONIN, COMMENT, PLACE },
    { REPLYOF_COMMENT_FILE, REPLYOF_COMMENT, COMMENT, COMMENT },
    { REPLYOF_POST_FILE, REPLYOF_POST, COMMENT, POST },

    { POST_HASCREATOR_FILE, POST_HASCREATOR, POST, PERSON },
    { POST_HASTAG_FILE, POST_HASTAG, POST, TAG },
    { POST_ISLOCATIONIN_FILE, POST_ISLOCATIONIN, POST, PLACE },

    { FORUM_CONTAINEROF_FILE, FORUM_CONTAINEROF, FORUM, POST },
    { FORUM_HASMODERATOR_FILE, FORUM_HASMODERATOR, FORUM, PERSON },
    { FORUM_HASTAG_FILE, FORUM_HASTAG, FORUM, TAG },

    { PERSON_HASINTEREST_FILE, PERSON_HASINTEREST, PERSON, TAG },
    { PERSON_ISLOCATEDIN_FILE, PERSON_ISLOCATEDIN, PERSON, PLACE },

    // edge with property
    { FORUM_HASMEMBER_FILE, FORUM_HASMEMBER, FORUM, PERSON, 1, false, sizeof(uint64_t) },
    { KNOWS_FILE, KNOWS, PERSON, PERSON, 1, false },  // undirected edge
    { LIKES_COMMENT_FILE, LIKES_COMMENT, PERSON, COMMENT, 1},
    { LIKES_POST_FILE, LIKES_POST, PERSON, POST, 1, false},
    { STUDYAT_FILE, STUDYAT, PERSON, ORGANISATION, 1},
    { WORKAT_FILE, WORKAT, PERSON, ORGANISATION, 1},
  };
}

vector<BenchLoader *>
LDBCRunner::make_loaders(int partition, MemDB *store) {
  vector<BenchLoader *> ret;

  // Relations (without properties)
  for (const EdgeDef& meta : edge_meta_) {
    ret.push_back(new TpEdgeLoader(meta.file_name, meta.etype,
                                 meta.src_vtype, meta.dst_vtype, meta.num_prop,
                                 meta.is_dir, 0, TP_EDGE_LOAD_PCT));
  }

  return ret;
}

vector<GraphLoader *>
LDBCRunner::make_graph_loaders(int partition,
                               GraphStore *graph_store,
                               RGMapping *mapping) {

  vector<GraphLoader *> ret;

  // Entities
  ret.push_back(new OrganisationLoader(ORGANISATION_FILE,
                              partition, graph_store, mapping));
  ret.push_back(new PlaceLoader(PLACE_FILE,
                              partition, graph_store, mapping));
  ret.push_back(new TagLoader(TAG_FILE,
                              partition, graph_store, mapping));
  ret.push_back(new TagClassLoader(TAGCLASS_FILE,
                                   partition, graph_store, mapping));
  ret.push_back(new PersonLoader(PERSON_FILE,
                                 partition, graph_store, mapping));
  ret.push_back(new CommentLoader(COMMENT_FILE,
                                  partition, graph_store, mapping));
  ret.push_back(new PostLoader(POST_FILE,
                               partition, graph_store, mapping));
  ret.push_back(new ForumLoader(FORUM_FILE,
                                partition, graph_store, mapping));

#if FRESHNESS == 0
  // Relations (without properties)
  for (const EdgeDef& meta : edge_meta_) {
    ret.push_back(new EdgeLoader(meta.file_name, meta.etype,
                                 meta.src_vtype, meta.dst_vtype, meta.num_prop,
                                 meta.is_dir, partition, GP_EDGE_LOAD_PCT,
                                 graph_store, mapping));
  }
#endif

  return ret;
}

void LDBCRunner::init_graph_store(GraphStore *graph_store,
                                  RGMapping *rg_map) {
  // max throughput of NO in a single machine
  uint64_t max_thpt_no_max = 1 * 1000 * 1000;
  const uint64_t time_scale = (((config.getRunSec() < 20)? 20 : config.getRunSec()) * max_thpt_no_max);

  // add schema
  SchemaImpl ldbc_schema;

  // vertex type
  ldbc_schema.label_id_map["organisation"] = ORGANISATION;
  ldbc_schema.label_id_map["place"] = PLACE;
  ldbc_schema.label_id_map["tag"] = TAG;
  ldbc_schema.label_id_map["tagclass"] = TAGCLASS;
  ldbc_schema.label_id_map["person"] = PERSON;
  ldbc_schema.label_id_map["comment"] = COMMENT;
  ldbc_schema.label_id_map["post"] = POST;
  ldbc_schema.label_id_map["knows"] = FORUM;

  // edge type
  ldbc_schema.label_id_map["org_islocationin"] = ORG_ISLOCATIONIN;
  ldbc_schema.label_id_map["ispartof"] = ISPARTOF;
  ldbc_schema.label_id_map["issubclassof"] = ISSUBCLASSOF;
  ldbc_schema.label_id_map["hastype"] = HASTYPE;
  ldbc_schema.label_id_map["comment_hascreator"] = COMMENT_HASCREATOR;
  ldbc_schema.label_id_map["comment_hastag"] = COMMENT_HASTAG;
  ldbc_schema.label_id_map["comment_islocationin"] = COMMENT_ISLOCATIONIN;
  ldbc_schema.label_id_map["replyof_comment"] = REPLYOF_COMMENT;
  ldbc_schema.label_id_map["replyof_post"] = REPLYOF_POST;
  ldbc_schema.label_id_map["post_hascreator"] = POST_HASCREATOR;
  ldbc_schema.label_id_map["post_hastag"] = POST_HASTAG;
  ldbc_schema.label_id_map["post_islocationin"] = POST_ISLOCATIONIN;
  ldbc_schema.label_id_map["forum_containerof"] = FORUM_CONTAINEROF;
  ldbc_schema.label_id_map["forum_hasmoderator"] = FORUM_HASMODERATOR;
  ldbc_schema.label_id_map["forum_hastag"] = FORUM_HASTAG;
  ldbc_schema.label_id_map["person_hasinterest"] = PERSON_HASINTEREST;
  ldbc_schema.label_id_map["person_islocationin"] = PERSON_ISLOCATEDIN;
  ldbc_schema.label_id_map["forum_hasmember"] = FORUM_HASMEMBER;
  ldbc_schema.label_id_map["knows"] = KNOWS;
  ldbc_schema.label_id_map["likes_comment"] = LIKES_COMMENT;
  ldbc_schema.label_id_map["likes_post"] = LIKES_POST;
  ldbc_schema.label_id_map["studyat"] = STUDYAT;
  ldbc_schema.label_id_map["workat"] = WORKAT;


  // comment & post
  int prop_offset = 0;

  ldbc_schema.vlabel2prop_offset[ORGANISATION] = prop_offset;
  ldbc_schema.property_id_map["org_id"] = Organisation::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["org_type"] = Organisation::Cid::TypeCol + prop_offset;
  ldbc_schema.property_id_map["org_name"] = Organisation::Cid::Name + prop_offset;
  ldbc_schema.property_id_map["org_url"] = Organisation::Cid::Url + prop_offset;
  prop_offset += Organisation::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[PLACE] = prop_offset;
  ldbc_schema.property_id_map["pla_id"] = Place::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["pla_type"] = Place::Cid::TypeCol + prop_offset;
  ldbc_schema.property_id_map["pla_name"] = Place::Cid::Name + prop_offset;
  ldbc_schema.property_id_map["pla_url"] = Place::Cid::Url + prop_offset;
  prop_offset += Place::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[TAG] = prop_offset;
  ldbc_schema.property_id_map["tag_id"] = Tag::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["tag_name"] = Tag::Cid::Name + prop_offset;
  ldbc_schema.property_id_map["tag_url"] = Tag::Cid::Url + prop_offset;
  prop_offset += Tag::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[TAGCLASS] = prop_offset;
  ldbc_schema.property_id_map["tagc_id"] = TagClass::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["tagc_name"] = TagClass::Cid::Name + prop_offset;
  ldbc_schema.property_id_map["tagc_url"] = TagClass::Cid::Url + prop_offset;
  prop_offset += TagClass::Cid::NumCol;

  // person
  ldbc_schema.vlabel2prop_offset[PERSON] = prop_offset;
  ldbc_schema.property_id_map["p_id"] = Person::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["p_first_name"] = Person::Cid::FistName + prop_offset;
  ldbc_schema.property_id_map["p_last_name"] = Person::Cid::LastName + prop_offset;
  ldbc_schema.property_id_map["p_gender"] = Person::Cid::Gender + prop_offset;
  ldbc_schema.property_id_map["p_birthday"] = Person::Cid::Birthday + prop_offset;
  ldbc_schema.property_id_map["p_browser_used"] = Person::Cid::BrowserUsed + prop_offset;
  ldbc_schema.property_id_map["p_location_ip"] = Person::Cid::LocationIP + prop_offset;
  ldbc_schema.property_id_map["p_creation_date"] = Person::Cid::CreationDate + prop_offset;
  ldbc_schema.property_id_map["p_pr_val"] = Person::Cid::PR + prop_offset;
  ldbc_schema.property_id_map["p_cc_val"] = Person::Cid::CC + prop_offset;
  ldbc_schema.property_id_map["p_sp_val"] = Person::Cid::SP + prop_offset;
  prop_offset += Person::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[COMMENT] = prop_offset;
  ldbc_schema.property_id_map["co_id"] = Comment::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["co_browser_used"] = Comment::Cid::BrowserUsed + prop_offset;;
  ldbc_schema.property_id_map["co_creation_date"] = Comment::Cid::CreationDate + prop_offset;;
  ldbc_schema.property_id_map["co_location_ip"] = Comment::Cid::LocationIP + prop_offset;;
  ldbc_schema.property_id_map["co_content"] = Comment::Cid::Content + prop_offset;;
  ldbc_schema.property_id_map["co_length"] = Comment::Cid::Length + prop_offset;;
  prop_offset += Comment::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[POST] = prop_offset;
  ldbc_schema.property_id_map["po_id"] = Post::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["po_browser_used"] = Post::Cid::BrowserUsed + prop_offset;
  ldbc_schema.property_id_map["po_creation_date"] = Post::Cid::CreationDate + prop_offset;
  ldbc_schema.property_id_map["po_location_ip"] = Post::Cid::LocationIP + prop_offset;
  ldbc_schema.property_id_map["po_content"] = Post::Cid::Content + prop_offset;
  ldbc_schema.property_id_map["po_length"] = Post::Cid::Length + prop_offset;
  ldbc_schema.property_id_map["po_language"] = Post::Cid::Language + prop_offset;
  ldbc_schema.property_id_map["po_image_file"] = Post::Cid::ImageFile + prop_offset;
  prop_offset += Post::Cid::NumCol;

  ldbc_schema.vlabel2prop_offset[FORUM] = prop_offset;
  ldbc_schema.property_id_map["fo_id"] = Forum::Cid::ID + prop_offset;
  ldbc_schema.property_id_map["fo_title"] = Forum::Cid::Title + prop_offset;
  ldbc_schema.property_id_map["fo_creation_date"] = Forum::Cid::CreationDate + prop_offset;
  prop_offset += Forum::Cid::NumCol;

  ldbc_schema.dtype_map[{ORGANISATION, 0}] = LONG;
  ldbc_schema.dtype_map[{ORGANISATION, 1}] = CHAR;
  ldbc_schema.dtype_map[{ORGANISATION, 2}] = STRING;
  ldbc_schema.dtype_map[{ORGANISATION, 3}] = STRING;

  ldbc_schema.dtype_map[{PLACE, 0}] = LONG;
  ldbc_schema.dtype_map[{PLACE, 1}] = CHAR;
  ldbc_schema.dtype_map[{PLACE, 2}] = STRING;
  ldbc_schema.dtype_map[{PLACE, 3}] = STRING;

  ldbc_schema.dtype_map[{TAG, 0}] = LONG;
  ldbc_schema.dtype_map[{TAG, 1}] = STRING;
  ldbc_schema.dtype_map[{TAG, 2}] = STRING;

  ldbc_schema.dtype_map[{TAGCLASS, 0}] = LONG;
  ldbc_schema.dtype_map[{TAGCLASS, 1}] = STRING;
  ldbc_schema.dtype_map[{TAGCLASS, 2}] = STRING;

  ldbc_schema.dtype_map[{PERSON, 0}] = LONG;
  ldbc_schema.dtype_map[{PERSON, 1}] = STRING;
  ldbc_schema.dtype_map[{PERSON, 2}] = STRING;
  ldbc_schema.dtype_map[{PERSON, 3}] = STRING;
  ldbc_schema.dtype_map[{PERSON, 4}] = DATE;
  ldbc_schema.dtype_map[{PERSON, 5}] = STRING;
  ldbc_schema.dtype_map[{PERSON, 6}] = STRING;
  ldbc_schema.dtype_map[{PERSON, 7}] = DATETIME;

  ldbc_schema.dtype_map[{COMMENT, 0}] = LONG;
  ldbc_schema.dtype_map[{COMMENT, 1}] = STRING;
  ldbc_schema.dtype_map[{COMMENT, 2}] = DATETIME;
  ldbc_schema.dtype_map[{COMMENT, 3}] = STRING;
  ldbc_schema.dtype_map[{COMMENT, 4}] = TEXT;
  ldbc_schema.dtype_map[{COMMENT, 5}] = INT;

  ldbc_schema.dtype_map[{POST, 0}] = LONG;
  ldbc_schema.dtype_map[{POST, 1}] = STRING;
  ldbc_schema.dtype_map[{POST, 2}] = DATETIME;
  ldbc_schema.dtype_map[{POST, 3}] = STRING;
  ldbc_schema.dtype_map[{POST, 4}] = TEXT;
  ldbc_schema.dtype_map[{POST, 5}] = INT;
  ldbc_schema.dtype_map[{POST, 6}] = STRING;
  ldbc_schema.dtype_map[{POST, 6}] = STRING;

  ldbc_schema.dtype_map[{FORUM, 0}] = LONG;
  ldbc_schema.dtype_map[{FORUM, 1}] = STRING;
  ldbc_schema.dtype_map[{FORUM, 2}] = STRING;

  graph_store->set_schema(ldbc_schema);

  for (int i = 0; i < VTYPE_NUM; ++i) {
    rg_map->define_vertex(i, i);  // (vlabel, table_id)
    graph_store->add_vgraph(i, rg_map);
  }
  graph_store->get_blob_json();

  for (const EdgeDef& m : edge_meta_) {
    // TODO: foreign key is unused, so set as 0
    rg_map->define_nn_edge(m.etype,
                           m.src_vtype,
                           m.dst_vtype,
                           0, 0,
                           !m.is_dir,
                           m.prop_size);
  }

  // add v-prop
  BackupDB::Schema schema;
  schema.sec_index_type = NO_BINDEX;
  switch(config.getBackupStoreType()) {
    case 0:
      schema.store_type = BSTORE_KV;
      break;
    case 1:
      schema.store_type = BSTORE_ROW;
      break;
    case 2:
      schema.store_type = BSTORE_COLUMN;
      break;
    case 3:
      schema.store_type = BSTORE_COLUMN2;
      break;
    default:
      printf("No backup store type: %d\n", config.getBackupStoreType());
      assert(false);
      break;
  }
  const size_t PAGE_SIZE = 4 * 1024;  // 1K items

  // organisation
  {
    Organisation::value p;
    schema.table_id = ORGANISATION;
    schema.klen = sizeof(Organisation::key);
    schema.cols =
        { { sizeof(p.id), false, 0, NO_BINDEX, LONG },
          { sizeof(p.type), false, 0, NO_BINDEX, CHAR },
          { sizeof(p.name), false, 0, NO_BINDEX, LONGSTRING },
          { sizeof(p.url), false, 0, NO_BINDEX, LONGSTRING },
        };
    schema.max_items = 7956;

    graph_store->add_vprop(ORGANISATION, schema);
  }

  // place
  {
    Place::value p;
    schema.table_id = PLACE;
    schema.klen = sizeof(Place::key);
    schema.cols =
        { { sizeof(p.id), false, 0, NO_BINDEX, LONG },
          { sizeof(p.type), false, 0, NO_BINDEX, CHAR },
          { sizeof(p.name), false, 0, NO_BINDEX, LONGSTRING },
          { sizeof(p.url), false, 0, NO_BINDEX, LONGSTRING },
        };
    schema.max_items = 1461;

    graph_store->add_vprop(PLACE, schema);
  }

  // tag
  {
    Tag::value t;
    schema.table_id = TAG;
    schema.klen = sizeof(Tag::key);
    schema.cols =
        { { sizeof(t.id), false, 0, NO_BINDEX, LONG },
          { sizeof(t.name), false, 0, NO_BINDEX, LONGSTRING },
          { sizeof(t.url), false, 0, NO_BINDEX, LONGSTRING },
        };
    schema.max_items = 16081;

    graph_store->add_vprop(TAG, schema);
  }

  // tagclass
  {
    TagClass::value tc;
    schema.table_id = TAGCLASS;
    schema.klen = sizeof(TagClass::key);
    schema.cols =
        { { sizeof(tc.id), false, 0, NO_BINDEX, LONG },
          { sizeof(tc.name), false, 0, NO_BINDEX, LONGSTRING },
          { sizeof(tc.url), false, 0, NO_BINDEX, LONGSTRING },
        };
    schema.max_items = 72;

    graph_store->add_vprop(TAGCLASS, schema);
  }

  // person
  {
    Person::value p;
    schema.table_id = PERSON;
    schema.klen = sizeof(Person::key);
    schema.cols =
        { { sizeof(p.id), false, 0, NO_BINDEX, LONG },
          { sizeof(p.firstName), false, 0, NO_BINDEX, STRING },
          { sizeof(p.lastName), false, 0, NO_BINDEX, STRING },
          { sizeof(p.gender), false, 0, NO_BINDEX, STRING },
          { sizeof(p.birthday), false, 0, NO_BINDEX, DATE },
          { sizeof(p.browserUsed), false, 0, NO_BINDEX, STRING },
          { sizeof(p.locationIP), false, 0, NO_BINDEX, STRING },
          { sizeof(p.creationDate), false, 0, NO_BINDEX, DATETIME },
#if 1
          { sizeof(p.pr_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(p.cc_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(p.sp_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(p.bf_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
#else
          { 4*sizeof(double), true, PAGE_SIZE / (4*sizeof(double)), NO_BINDEX, DOUBLE_LIST},
#endif
        };
    // schema.max_items = time_scale;
    schema.max_items = 66000;

    graph_store->add_vprop(PERSON, schema);
  }

  // comment
  {
    Comment::value c;
    schema.table_id = COMMENT;
    schema.klen = sizeof(Comment::key);
    schema.cols =
        { { sizeof(c.id), false, 0, NO_BINDEX, LONG },
          { sizeof(c.browserUsed), false, 0, NO_BINDEX, STRING },
          { sizeof(c.creationDate), false, 0, NO_BINDEX, DATETIME },
          { sizeof(c.locationIP), false, 0, NO_BINDEX, STRING },
          { sizeof(c.content), false, 0, NO_BINDEX, TEXT },
          { sizeof(c.length), false, 0, NO_BINDEX, INT },
        };
    schema.max_items = 22000000;

    graph_store->add_vprop(COMMENT, schema);
  }

  // post
  {
    Post::value po;
    schema.table_id = POST;
    schema.klen = sizeof(Post::key);
    schema.cols =
        { { sizeof(po.id), false, 0, NO_BINDEX, LONG },
          { sizeof(po.browserUsed), false, 0, NO_BINDEX, STRING },
          { sizeof(po.creationDate), false, 0, NO_BINDEX, DATETIME },
          { sizeof(po.locationIP), false, 0, NO_BINDEX, STRING },
          { sizeof(po.content), false, 0, NO_BINDEX, TEXT },
          { sizeof(po.length), false, 0, NO_BINDEX, INT },
          { sizeof(po.language), false, 0, NO_BINDEX, STRING },
          { sizeof(po.imageFile), false, 0, NO_BINDEX, STRING },
#if 1
          { sizeof(po.pr_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(po.cc_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(po.sp_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
          { sizeof(po.bf_val), true, PAGE_SIZE / sizeof(double), NO_BINDEX, DOUBLE },
#else
          { 4*sizeof(double), true, PAGE_SIZE / (4*sizeof(double)), NO_BINDEX, DOUBLE_LIST},
#endif
        };
    schema.max_items = 7500000;

    graph_store->add_vprop(POST, schema);
  }

  // forum
  {
    Forum::value f;
    schema.table_id = FORUM;
    schema.klen = sizeof(Forum::key);
    schema.cols =
        { { sizeof(f.id), false, 0, NO_BINDEX, LONG },
          { sizeof(f.title), false, 0, NO_BINDEX, LONGSTRING },
          { sizeof(f.creationDate), false, 0, NO_BINDEX, DATETIME },
        };
    schema.max_items = 600000;

    graph_store->add_vprop(FORUM, schema);
  }
}

vector<BenchWorker *> LDBCRunner::make_workers() {
  int rest_worker = config.getNumTxnThreads();
  vector<BenchWorker *> ret;

  int mac_id = config.getServerID();
  vector<int> primaries = view.get_primaries(mac_id);
  assert(primaries.size() == 1);  // XXX: Todo

  int start_pid = primaries[0];
  fast_random r(23984543 + start_pid);

  for (int i = 0; i < rest_worker; ++i) {
      printf("[LDBCWorker] worker %d\n", i);
      ret.push_back(new LDBCWorker(i, r.next(), stores_[0]));

  }
  return ret;
}

vector<BenchClient *> LDBCRunner::make_clients() {
  vector<BenchClient *> ret;
  int num_txn_workers = config.getNumTxnThreads();
  // ret.push_back(new LDBCClient(num_txn_workers));
  ret.push_back(new BenchClient(num_txn_workers));
  return ret;
}

vector<QueryWorker *> LDBCRunner::make_qry_workers() {
  vector<QueryWorker *> ret;
  ret.push_back(new LDBCQueryWorker(0, 1, 23984543, graph_stores_[0], rg_maps_[0]));
  return ret;
}

vector<AnalyticsWorker*> LDBCRunner::make_ana_workers() {
  std::vector<AnalyticsWorker*> ret;

#ifdef WITH_GRAPE
  // init grape engine environment
  google::InitGoogleLogging("analytical_apps");
  if(config.isUseGrapeEngine()) {
    grape::GrapeEngine::Init();
  }
#endif

  printf("[LDBCRunner] Make analytics workers\n");

  int num_ana_threads = config.getNumAnalyticsThreads();
  int num_ana_session = config.getAnalyticsSession();

  if (num_ana_threads == 0 || num_ana_session == 0) return ret;

  int num_ana_threads_per_session = num_ana_threads / num_ana_session;

  if(num_ana_threads_per_session == 0) return ret;

  bool use_seg_graph = config.isUseSegGraph();

  for (int i = 0; i < num_ana_session; i++) {
    int worker_idx = i * num_ana_threads_per_session;
    ret.push_back(new LDBCAnalyticsWorker(worker_idx,
                                        num_ana_threads_per_session,
                                        use_seg_graph,
                                        this->graph_stores_[0],
                                        this->rg_maps_[0]));
  }

  return ret;
}

vector<LogWorker *> LDBCRunner::make_backup_workers() {
  int backup_nthreads = config.getNumBackupThreads();
  vector<LogWorker *> ret;

  printf("[Runner] Make backup workers\n");
  LogCleaner* log_cleaner = new LDBCLogCleaner;

  for (BackupDB *db : backup_stores_) {
    assert(db);
    log_cleaner->add_backup_store(db);
  }

  assert(graph_stores_.size() == rg_maps_.size());
  for (int i = 0; i < rg_maps_.size(); ++i) {
    log_cleaner->add_graph_store(graph_stores_[i], rg_maps_[i]);
  }

  DBLogger::set_log_cleaner(log_cleaner);
  for (uint i = 0; i < backup_nthreads; i++) {
    ret.push_back(new LogWorker(i));
  }

  return ret;
}

void LDBCRunner::warmup_buffer(char *buffer) {
  int num_mac = config.getNumServers();
  LogTSManager::initialize_meta_data(buffer, num_mac);
#ifdef SI_TX
  TSManager::initilize_meta_data(buffer, num_mac);
#endif
}


}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc

