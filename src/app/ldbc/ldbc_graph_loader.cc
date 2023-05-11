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

#include "ldbc_graph_loader.h"
#include "ldbc_schema.h"

#include <fstream>
#include <sstream>

using namespace std;
using namespace nocc::graph;
using namespace livegraph;

namespace {
  const char CSV_DELIMITER = '|';

  void split(const string &str, char delimiter, vector<string> &v) {
    vector<string> res;
    string word;

    v.clear();
    istringstream strstr(str);
    while(getline(strstr, word, delimiter)) {
      v.push_back(word);
    }
  }
}

namespace nocc {
namespace oltp {
namespace ldbc {

void OrganisationLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Organisation::NumCol);
  cids.assign(Organisation::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "type", "name", "url"};
  for (int i = 0; i < Organisation::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")         cid_idx = Organisation::ID;
    else if (csv_header[i] == "name")  cid_idx = Organisation::Name;
    else if (csv_header[i] == "url")   cid_idx = Organisation::Url;
    else if (csv_header[i] == "type")  cid_idx = Organisation::TypeCol;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void OrganisationLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = ORGANISATION;

  if (!csv) {
    printf("[OrganisationLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[OrganisationLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto writer = graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Organisation::key key;
    Organisation::value value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      switch (cids[word_idx]) {
        case Organisation::ID:
          key.id = static_cast<ID> (stoll(word));
          value.id = key.id;
          break;
        case Organisation::Name:
          value.name.assign(word);
          break;
        case Organisation::Url:
          value.url.assign(word);
          break;
        case Organisation::TypeCol:
        {
          if (word == "company") value.type = Organisation::COMPANY;
          else if (word == "university") value.type = Organisation::UNIVERSITY;
          else {
            printf("[PsersonLoader] Error type %s\n", word.c_str());
            assert(false);
          }
          break;
        }
        default:
          printf("[PsersonLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Organisation::NumCol);

    // insert vertex
    vertex_t v = writer.new_vertex();

    // insert properties
    auto off = property->getNewOffset();
    assert(off == v);
    property->insert(v, key.id, (char *) &value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, key.id, (uint64_t) v);
    max_vid = v;
    ++num_lines;
  }
  printf("[OrganisationLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}

void PlaceLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Place::NumCol);
  cids.assign(Place::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "name", "url", "type" };
  for (int i = 0; i < Place::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")         cid_idx = Place::ID;
    else if (csv_header[i] == "name")  cid_idx = Place::Name;
    else if (csv_header[i] == "url")   cid_idx = Place::Url;
    else if (csv_header[i] == "type")  cid_idx = Place::TypeCol;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void PlaceLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = PLACE;

  if (!csv) {
    printf("[PlaceLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[PlaceLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto writer = graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Place::key key;
    Place::value value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      switch (cids[word_idx]) {
        case Place::ID:
          key.id = static_cast<ID> (stoll(word));
          value.id = key.id;
          break;
        case Place::Name:
          value.name.assign(word);
          break;
        case Place::Url:
          value.url.assign(word);
          break;
        case Place::TypeCol:
        {
          if (word == "country") value.type = Place::COUNTRY;
          else if (word == "city") value.type = Place::CITY;
          else if (word == "continent") value.type = Place::CONTINENT;
          else {
            printf("[PsersonLoader] Error type %s\n", word.c_str());
            assert(false);
          }
          break;
        }
        default:
          printf("[PsersonLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Place::NumCol);

    // insert vertex
    vertex_t v = writer.new_vertex();

    // insert properties
    auto off = property->getNewOffset();
    assert(off == v);
    property->insert(v, key.id, (char *) &value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, key.id, (uint64_t) v);
    max_vid = v;
    ++num_lines;
  }
  printf("[PlaceLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}

void TagLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Tag::NumCol);
  cids.assign(Tag::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "name", "url" };
  for (int i = 0; i < Tag::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")        cid_idx = Tag::ID;
    else if (csv_header[i] == "name") cid_idx = Tag::Name;
    else if (csv_header[i] == "url")  cid_idx = Tag::Url;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void TagLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = TAG;

  if (!csv) {
    printf("[TagLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[TagLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto writer = graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Tag::key key;
    Tag::value value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      switch (cids[word_idx]) {
        case Tag::ID:
          key.id = static_cast<ID> (stoll(word));
          value.id = key.id;
          break;
        case Tag::Name:
          value.name.assign(word);
          break;
        case Tag::Url:
          value.url.assign(word);
          break;
        default:
          printf("[PsersonLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Tag::NumCol);

    // insert vertex
    vertex_t v = writer.new_vertex();

    // insert properties
    auto off = property->getNewOffset();
    assert(off == v);
    property->insert(v, key.id, (char *) &value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, key.id, (uint64_t) v);
    max_vid = v;
    ++num_lines;
  }
  printf("[TagLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}

void TagClassLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == TagClass::NumCol);
  cids.assign(TagClass::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "name", "url" };
  for (int i = 0; i < TagClass::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")        cid_idx = TagClass::ID;
    else if (csv_header[i] == "name") cid_idx = TagClass::Name;
    else if (csv_header[i] == "url")  cid_idx = TagClass::Url;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void TagClassLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = TAGCLASS;

  if (!csv) {
    printf("[TagClassLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[TagClassLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto writer = graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    TagClass::key key;
    TagClass::value value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      switch (cids[word_idx]) {
        case TagClass::ID:
          key.id = static_cast<ID> (stoll(word));
          value.id = key.id;
          break;
        case TagClass::Name:
          value.name.assign(word);
          break;
        case TagClass::Url:
          value.url.assign(word);
          break;
        default:
          printf("[PsersonLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == TagClass::NumCol);

    // insert vertex
    vertex_t v = writer.new_vertex();

    // insert properties
    auto off = property->getNewOffset();
    assert(off == v);
    property->insert(v, key.id, (char *) &value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, key.id, (uint64_t) v);
    max_vid = v;
    ++num_lines;
  }
  printf("[TagClassLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}
  
void PersonLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  // NOTE: Person has three extra AP cols
  assert(headers.size() == (Person::NumCol-4));
  cids.assign(Person::NumCol-4, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "firstName", "lastName", "gender",
                          "birthday", "creationDate", "locationIP",
                          "browserUsed" };
  for (int i = 0; i < Person::NumCol-4; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")             cid_idx = Person::ID;
    else if (csv_header[i] == "firstName") cid_idx = Person::FistName;
    else if (csv_header[i] == "lastName")  cid_idx = Person::LastName;
    else if (csv_header[i] == "gender")    cid_idx = Person::Gender;
    else if (csv_header[i] == "birthday")  cid_idx = Person::Birthday;
    else if (csv_header[i] == "browserUsed")  cid_idx = Person::BrowserUsed;
    else if (csv_header[i] == "locationIP")  cid_idx = Person::LocationIP;
    else if (csv_header[i] == "creationDate")  cid_idx = Person::CreationDate;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void PersonLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = PERSON;

  if (!csv) {
    printf("[PersonLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[PersonLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *p_graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *p_property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto p_writer = p_graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Person::key p_key;
    Person::value p_value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      // TODO
      switch (cids[word_idx]) {
        case Person::ID:
          p_key.id = static_cast<ID> (stoll(word));
          p_value.id = p_key.id;
          break;
        case Person::FistName:
          p_value.firstName.assign(word);
          break;
        case Person::LastName:
          p_value.lastName.assign(word);
          break;
        case Person::Gender:
          p_value.gender.assign(word);
          break;
        case Person::Birthday:
          p_value.birthday.assign(word);
          break;
        case Person::BrowserUsed:
          p_value.browserUsed.assign(word);
          break;
        case Person::LocationIP:
          p_value.locationIP.assign(word);
          break;
        case Person::CreationDate:
          p_value.creationDate.assign(word);
          break;
        default:
          printf("[PsersonLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Person::NumCol-4);

    // insert vertex
    vertex_t p_v = p_writer.new_vertex();

    // init AP col
    p_value.pr_val = 1.0;
    p_value.cc_val = p_v;
    p_value.sp_val = 1000000000;
    p_value.bf_val = 1000;

    // insert properties
    auto off = p_property->getNewOffset();
    assert(off == p_v);
    p_property->insert(p_v, p_key.id, (char *) &p_value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, p_key.id, (uint64_t) p_v);
    max_vid = p_v;
    ++num_lines;
  }
  printf("[PersonLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}
  
void CommentLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Comment::NumCol);
  cids.assign(Comment::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "creationDate", "locationIP", "browserUsed",
                          "content", "length" };
  for (int i = 0; i < Comment::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")             cid_idx = Comment::ID;
    else if (csv_header[i] == "creationDate")  cid_idx = Comment::CreationDate;
    else if (csv_header[i] == "locationIP")  cid_idx = Comment::LocationIP;
    else if (csv_header[i] == "browserUsed")  cid_idx = Comment::BrowserUsed;
    else if (csv_header[i] == "content")   cid_idx = Comment::Content;
    else if (csv_header[i] == "length")  cid_idx = Comment::Length;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void CommentLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = COMMENT;

  if (!csv) {
    printf("[CommentLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[CommentLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *c_graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *c_property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto c_writer = c_graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Comment::key c_key;
    Comment::value c_value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      // TODO
      switch (cids[word_idx]) {
        case Comment::ID:
          c_key.id = static_cast<ID> (stoll(word));
          c_value.id = c_key.id;
          break;
        case Comment::BrowserUsed:
          c_value.browserUsed.assign(word);
          break;
        case Comment::CreationDate:
          c_value.creationDate.assign(word);
          break;
        case Comment::LocationIP:
          c_value.locationIP.assign(word);
          break;
        case Comment::Content:
          c_value.content.assign(word);
          break;
        case Comment::Length:
          c_value.length = static_cast<Uint> (stoll(word));
          break;
        default:
          printf("[CommentLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Comment::NumCol);

    // insert vertex
    vertex_t c_v = c_writer.new_vertex();

    // insert properties
    auto off = c_property->getNewOffset();
    assert(off == c_v);
    c_property->insert(c_v, c_key.id, (char *) &c_value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, c_key.id, (uint64_t) c_v);
    max_vid = c_v;
    ++num_lines;
  }
  printf("[CommentLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}
  
void PostLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == (Post::NumCol-4));
  cids.assign((Post::NumCol-4), -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "imageFile", "creationDate", "locationIP", 
                          "browserUsed", "language", "content", "length" };
  for (int i = 0; i < (Post::NumCol-4); ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")             cid_idx = Post::ID;
    else if (csv_header[i] == "creationDate")  cid_idx = Post::CreationDate;
    else if (csv_header[i] == "locationIP")  cid_idx = Post::LocationIP;
    else if (csv_header[i] == "browserUsed")  cid_idx = Post::BrowserUsed;
    else if (csv_header[i] == "content")   cid_idx = Post::Content;
    else if (csv_header[i] == "length")  cid_idx = Post::Length;
    else if (csv_header[i] == "language")  cid_idx = Post::Language;
    else if (csv_header[i] == "imageFile")  cid_idx = Post::ImageFile;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void PostLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = POST;

  if (!csv) {
    printf("[PostLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[PostLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *po_graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *po_property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto po_writer = po_graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Post::key po_key;
    Post::value po_value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      // TODO
      try{
      switch (cids[word_idx]) {
        case Post::ID:
          po_key.id = static_cast<ID> (stoll(word));
          po_value.id = po_key.id;
          break;
        case Post::BrowserUsed:
          po_value.browserUsed.assign(word);
          break;
        case Post::CreationDate:
          po_value.creationDate.assign(word);
          break;
        case Post::LocationIP:
          po_value.locationIP.assign(word);
          break;
        case Post::Content:
          po_value.content.assign(word);
          break;
        case Post::Length:
          po_value.length = static_cast<Uint> (stoll(word));
          break;
        case Post::Language:
          po_value.language.assign(word);
          break;
        case Post::ImageFile:
          po_value.imageFile.assign(word);
          break;
        default:
          printf("[PostLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }
      } catch (exception &e) {
        printf("[PostLoader] Error parse!\n");
        cout << e.what() << endl;
        assert(false);
      }


      ++word_idx;
    }
    assert(word_idx == (Post::NumCol-4));

    // insert vertex
    vertex_t po_v = po_writer.new_vertex();

    // init AP col
    po_value.pr_val = 1.0;
    po_value.cc_val = po_v;
    po_value.sp_val = 1000000000;
    po_value.bf_val = 1000;

    // insert properties
    auto off = po_property->getNewOffset();
    assert(off == po_v);
    po_property->insert(po_v, po_key.id, (char *) &po_value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, po_key.id, (uint64_t) po_v);
    max_vid = po_v;
    ++num_lines;
  }
  printf("[PostLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}
  
void ForumLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Forum::NumCol);
  cids.assign(Forum::NumCol, -1);

  // this array is according to the csv file
  string csv_header[] = { "id", "title", "creationDate" };
  for (int i = 0; i < Forum::NumCol; ++i) {
    int cid_idx = -1;
    if (csv_header[i] == "id")             cid_idx = Forum::ID;
    else if (csv_header[i] == "title")  cid_idx = Forum::Title;
    else if (csv_header[i] == "creationDate")  cid_idx = Forum::CreationDate;

    assert(cid_idx != -1 && cids[i] == -1);
    cids[i] = cid_idx;
  }

  for (int cid :cids) assert(cid != -1);
}

template <class GraphType>
void ForumLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0, max_vid;
  const int vertex_type = FORUM;

  if (!csv) {
    printf("[ForumLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[ForumLoader] Start load file %s.\n",  csv_file_.c_str());
  
  GraphType *graph = graph_store->get_graph<GraphType>(vertex_type);
  BackupStore *property = graph_store->get_property(vertex_type);

  // this `auto` is necessary, the write may Transaction or EpochGraphWriter
  auto writer = graph->create_graph_writer(write_epoch);  // write epoch

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Forum::key key;
    Forum::value value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      try{
      switch (cids[word_idx]) {
        case Forum::ID:
          key.id = static_cast<ID> (stoll(word));
          value.id = key.id;
          break;
        case Forum::Title:
          value.title.assign(word);
          break;
        case Forum::CreationDate:
          value.creationDate.assign(word);
          break;
        default:
          printf("[ForumLoader] Error column id %d\n", cids[word_idx]);
          assert(false);
      }
      } catch (exception &e) {
        printf("[ForumLoader] Error parse!\n");
        cout << e.what() << endl;
        assert(false);
      }

      ++word_idx;
    }
    assert(word_idx == Forum::NumCol);

    // insert vertex
    vertex_t v = writer.new_vertex();

    // insert properties
    auto off = property->getNewOffset();
    assert(off == v);
    property->insert(v, key.id, (char *) &value, write_seq, write_epoch);

    mapping_->set_key2vid(vertex_type, key.id, (uint64_t) v);
    max_vid = v;
    ++num_lines;
  }
  printf("[ForumLoader] End loading %lu vertices, max id %lu\n",
         num_lines, max_vid);
}

#if 0
void 
KnowsLoader::parse_cid_(const vector<string> &headers, vector<int> &cids) {
  assert(headers.size() == Knows::NumCol);
  cids.assign(Knows::NumCol, -1);

  // this array is according to the csv file
  cids[0] = Knows::Person1ID;
  cids[1] = Knows::Person2ID;
  cids[2] = Knows::CreationDate;
}

template <class GraphType>
void KnowsLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0;
  const int src_type = PERSON;
  const int dst_type = PERSON;

  if (!csv) {
    printf("[KnowsLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  printf("[KnowsLoader] Start load file %s.\n",  csv_file_.c_str());

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  parse_cid_(headers, cids);
    
  GraphType *p_graph = graph_store->get_graph<GraphType>(src_type);
  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto p_writer = p_graph->create_graph_writer(write_epoch);  // write epoch

  // parse the file
  while (getline(csv, line)) {
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    Knows::key k_key;
    Knows::value k_value;

    // parse each line
    while (getline(strstr, word, CSV_DELIMITER)) {
      // TODO
      switch (cids[word_idx]) {
        case Knows::Person1ID:
          k_key.person1id = static_cast<ID> (stoll(word));
          break;
        case Knows::Person2ID:
          k_key.person2id = static_cast<ID> (stoll(word));
          break;
        case Knows::CreationDate:
          k_value.creationDate.assign(word);
          break;
      }

      ++word_idx;
    }
    assert(word_idx == Knows::NumCol);

    // insert edge
    uint64_t p1_vid = mapping_->get_key2vid(src_type, k_key.person1id),
             p2_vid = mapping_->get_key2vid(dst_type, k_key.person2id);
    p_writer.put_edge(p1_vid, KNOWS, EOUT, p2_vid);
    p_writer.put_edge(p2_vid, KNOWS, EIN, p1_vid);

    // TODO: insert edge properties
    // p_property->insert(p_v, p_key.id, (char *) &p_value, write_seq, write_epoch);

    ++num_lines;
  }
  printf("[KnowsLoader] End loading %lu edges\n", num_lines);
}
#endif

static int lines_of_data(string prefix, string file_name) {
  ifstream csv(file_name);
  string line;
  int line_number = 0;

  if (!csv) {
    printf("[%s] File %s not exist!\n", prefix.c_str(), file_name.c_str());
    assert(false);
  }

  while (getline(csv, line)) ++line_number;

  if (line_number <= 1) {
    printf("[%s] File %s empty data!\n", prefix.c_str(), file_name.c_str());
    assert(false);
  }

  return (line_number - 1);  // one line is header
}

vector<string> EdgeLoader::old_inserted_edges[];
vector<string> TpEdgeLoader::inserted_edges[];

template <class GraphType>
void EdgeLoader::load_graph(graph::GraphStore* graph_store) {
  const int write_epoch = 0, write_seq = 0;
  string line;
  vector<string> headers;
  // vector<int> cids;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0;
  const int src_type = src_vtype_;
  const int dst_type = dst_vtype_;

  if (!csv) {
    printf("[EdgeLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  
  int line_data = lines_of_data("EdgeLoader", csv_file_);

  printf("[EdgeLoader] Start load file %s with %d lines of data.\n",
         csv_file_.c_str(), line_data);

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  // parse_cid_(headers, cids);
  assert(headers.size() == 2 + num_prop_);  // src key, dst key
    
  GraphType *p_src_graph = graph_store->get_graph<GraphType>(src_type);
  GraphType *p_dst_graph = graph_store->get_graph<GraphType>(dst_type);
  // this `auto` is necessary, the p_write may Transaction or EpochGraphWriter
  auto p_src_writer = p_src_graph->create_graph_writer(write_epoch);  // write epoch
  auto p_dst_writer = p_dst_graph->create_graph_writer(write_epoch);  // write epoch

  // parse the file
  while (getline(csv, line)) {
    if (num_lines == int(line_data * load_pct_ / 100.0))
      break;
    istringstream strstr(line);
    string word;
    int word_idx = 0;
    ID src_key = ID(-1), dst_key = ID(-1);

    // parse each line
    string buf;
    while (getline(strstr, word, CSV_DELIMITER)) {
      ID key = static_cast<ID> (stoll(word));
      if (word_idx == 0) {
        src_key = key;
      } else if (word_idx == 1) {
        dst_key = key;
      } else if (word_idx == 2) {
        deal_prop(word, buf);
      }
      ++word_idx;
    }
    assert(word_idx == 2 + num_prop_);

    // insert edge
    uint64_t p1_vid = mapping_->get_key2vid(src_type, src_key),
             p2_vid = mapping_->get_key2vid(dst_type, dst_key);

#ifdef WITH_EDGE_PROP
    if(elabel_ == LIKES_POST) {
      double distance = 1.0;
      std::string_view edge_data((char*)&distance, sizeof(double));
      p_src_writer.put_edge(p1_vid, elabel_, EOUT, p2_vid, edge_data);
      p_dst_writer.put_edge(p2_vid, elabel_, EIN, p1_vid, edge_data);

      if (!is_dir_) {
        // add another direction for undirected edges
        p_dst_writer.put_edge(p2_vid, elabel_, EOUT, p1_vid, edge_data);  // src of another dir
        p_src_writer.put_edge(p1_vid, elabel_, EIN, p2_vid, edge_data);   // dst of another dir
      }
    } else {
      p_src_writer.put_edge(p1_vid, elabel_, EOUT, p2_vid);
      p_dst_writer.put_edge(p2_vid, elabel_, EIN, p1_vid);

      if (!is_dir_) {
        // add another direction for undirected edges
        p_dst_writer.put_edge(p2_vid, elabel_, EOUT, p1_vid);  // src of another dir
        p_src_writer.put_edge(p1_vid, elabel_, EIN, p2_vid);   // dst of another dir
      }
    }
#else
    p_src_writer.put_edge(p1_vid, elabel_, EOUT, p2_vid);
    p_dst_writer.put_edge(p2_vid, elabel_, EIN, p1_vid);

    if (!is_dir_) {
      // add another direction for undirected edges
      p_dst_writer.put_edge(p2_vid, elabel_, EOUT, p1_vid);  // src of another dir
      p_src_writer.put_edge(p1_vid, elabel_, EIN, p2_vid);   // dst of another dir
    }
#endif

    // TODO: insert edge properties
    string_view prop(buf);
    // p_property->insert(p_v, p_key.id, (char *) &p_value, write_seq, write_epoch);
    
    // insert to the vector for trasnaction
    old_inserted_edges[elabel_].emplace_back(EdgeProp::total_size(buf.size()), 0);
    string &str = old_inserted_edges[elabel_].back();
    EdgeProp::fill_str(src_key, dst_key, buf, str);

    ++num_lines;
  }
  assert(old_inserted_edges[elabel_].size() == num_lines);
  assert(num_lines == int(line_data * load_pct_ / 100.0));
  printf("[EdgeLoader] End loading %lu edges\n", num_lines);
}

void TpEdgeLoader::load() {
  string line;
  vector<string> headers;
  ifstream csv(csv_file_);
  uint64_t num_lines = 0;
  uint64_t num_rows = 0;
  const int src_type = src_vtype_;
  const int dst_type = dst_vtype_;

  if (!csv) {
    printf("[TpEdgeLoader] File %s not exist!\n", csv_file_.c_str());
    assert(false);
  }
  
  int line_data = lines_of_data("TpEdgeLoader", csv_file_);

  printf("[TpEdgeLoader] Start load file %s.\n",  csv_file_.c_str());

  getline(csv, line);
  split(line, CSV_DELIMITER, headers);
  // parse_cid_(headers, cids);
  assert(headers.size() == 2 + num_prop_);  // src key, dst key
    
  // parse the file
  while (getline(csv, line)) {
    if (num_rows < int(line_data * (100 - load_pct_) / 100.0)) {
      ++num_rows;
      continue;
    }

    istringstream strstr(line);
    string word;
    int word_idx = 0;
    ID src_key = ID(-1), dst_key = ID(-1);

    // parse each line
    string buf;
    while (getline(strstr, word, CSV_DELIMITER)) {
      ID key = static_cast<ID> (stoll(word));
      if (word_idx == 0) {
        src_key = key;
      } else if (word_idx == 1) {
        dst_key = key;
      } else if (word_idx == 2) {
        deal_prop(word, buf);
      }
      ++word_idx;
    }
    assert(word_idx == 2 + num_prop_);

#if 0
    // insert edge
    uint64_t p1_vid = mapping_->get_key2vid(src_type, src_key),
             p2_vid = mapping_->get_key2vid(dst_type, dst_key);
    p_src_writer.put_edge(p1_vid, elabel_, EOUT, p2_vid);
    p_dst_writer.put_edge(p2_vid, elabel_, EIN, p1_vid);

    if (!is_dir_) {
      // add another direction for undirected edges
      p_dst_writer.put_edge(p2_vid, elabel_, EOUT, p1_vid);  // src of another dir
      p_src_writer.put_edge(p1_vid, elabel_, EIN, p2_vid);   // dst of another dir
    }
#endif

#if FRESHNESS == 1
    // resize the property as the timestamp for freshness
    if (elabel_ == FORUM_HASMEMBER) {
      buf.resize(sizeof(uint64_t));
      uint64_t f_id = 893353198399l;  // SF = 0.1
      if (src_key == f_id) {
        printf("traced key vec idx %ld\n", inserted_edges[elabel_].size());
      }
      // uint64_t *ptr = (uint64_t *) &buf[0];  // timestamp
      // *ptr = rdtsc();
    }
#endif

    // TODO: insert edge properties
    string_view prop(buf);
    // p_property->insert(p_v, p_key.id, (char *) &p_value, write_seq, write_epoch);
    
    // insert to the vector for trasnaction
    inserted_edges[elabel_].emplace_back(EdgeProp::total_size(buf.size()), 0);
    string &str = inserted_edges[elabel_].back();
    EdgeProp::fill_str(src_key, dst_key, buf, str);

    ++num_lines;
  }
  assert(inserted_edges[elabel_].size() == num_lines);
  assert (num_rows == int(line_data * (100 - load_pct_) / 100.0));
  // assert(num_lines == int(line_data * load_pct_ / 100.0));  // precision
  printf("[TpEdgeLoader] End loading %lu edges\n", num_lines);
}

static void deal_edge_prop(int elabel_, const string &word, string &buf) {
  try {
  switch (elabel_) {
    case FORUM_HASMEMBER:
    case KNOWS:
    case LIKES_COMMENT:
    case LIKES_POST:
    {
      DateTime dt;
      dt.assign(word);
      buf.assign((char *) &dt, sizeof(dt));
      break;
    }
    case STUDYAT:
    case WORKAT:
    {
      Uint year = static_cast<uint32_t> (stoll(word));
      buf.assign((char *) &year, sizeof(year));
      break;
    }
    default:
      printf("[EdgeLoader] Error property of elabel %d.\n",  elabel_);
      assert(false);
  }

  } catch (exception &e) {
    printf("[PostLoader] Error parse!\n");
    cout << e.what() << endl;
    assert(false);
  }
}

void EdgeLoader::deal_prop(const string &word, string &buf) {
  deal_edge_prop(elabel_, word, buf);
}

void TpEdgeLoader::deal_prop(const string &word, string &buf) {
  deal_edge_prop(elabel_, word, buf);
}

#define LoadGraph(label) \
  template void \
  label##Loader::load_graph<livegraph::SegGraph>(graph::GraphStore* graph_store); \
  \
  template void \
  label##Loader::load_graph<livegraph::Graph>(graph::GraphStore* graph_store);

LoadGraph(Organisation);
LoadGraph(Place);
LoadGraph(Tag);
LoadGraph(TagClass);

LoadGraph(Person);
LoadGraph(Comment);
LoadGraph(Post);
LoadGraph(Forum);

// LoadGraph(Knows);
LoadGraph(Edge);

}  // namespace ldbc
}  // namespace oltp
}  // namespace nocc
