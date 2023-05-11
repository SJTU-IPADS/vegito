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

#pragma once

#include "framework/utils/inline_str.h"

namespace nocc {
namespace oltp {
namespace ldbc {

typedef uint64_t ID;
typedef uint32_t Uint;
typedef inline_str_8<40>    String;
typedef inline_str_8<256>   LongString;
typedef inline_str_16<2000> Text;
typedef inline_str_fixed<10> Date;      // yyyy-mm-dd
typedef inline_str_fixed<28> DateTime;  // yyyy-mm-ddTHH:MM:ss.sss+0000
                      // specificiation is yyyy-mm-ddTHH:MM:ss.sss+00:00 

enum VType {
  ORGANISATION, PLACE, TAG, TAGCLASS,
  PERSON, COMMENT, POST, FORUM,
  VTYPE_NUM
};

enum ETpye {
  // static (all simple)
  ORG_ISLOCATIONIN = VTYPE_NUM,
  ISPARTOF,
  ISSUBCLASSOF,
  HASTYPE,

  // from comment (all simple)
  COMMENT_HASCREATOR,
  COMMENT_HASTAG,
  COMMENT_ISLOCATIONIN,
  REPLYOF_COMMENT,
  REPLYOF_POST,

  // from post (all simple)
  POST_HASCREATOR,
  POST_HASTAG,
  POST_ISLOCATIONIN,

  // from forum
  FORUM_CONTAINEROF,
  FORUM_HASMODERATOR,
  FORUM_HASTAG,

  // from person
  PERSON_HASINTEREST,
  PERSON_ISLOCATEDIN,

  // dynmic and property
  FORUM_HASMEMBER,
  KNOWS,
  LIKES_COMMENT,
  LIKES_POST,
  STUDYAT,
  WORKAT,
  
  // number of labels
  ETYPE_NUM
};

/************** Entity ****************/
// Static
struct Organisation {
  enum Type : uint8_t {
    COMPANY, UNIVERSITY
  };

  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID         id;
    Type       type;
    LongString name;
    LongString url;
  };

  enum Cid {
    ID, TypeCol, Name, Url, NumCol
  };
};

struct Place {
  enum Type : uint8_t {
    CONTINENT, COUNTRY, CITY
  };

  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID         id;
    Type       type;
    LongString name;
    LongString url;
  };

  enum Cid {
    ID, TypeCol, Name, Url, NumCol
  };
};

struct Tag {
  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID         id;
    LongString name;
    LongString url;
  }; 

  enum Cid {
    ID, Name, Url, NumCol
  };
};

typedef Tag TagClass;

// Dynamic
struct Person {
  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID       id;
    String   firstName;
    String   lastName;
    String   gender;
    Date     birthday;
    String   browserUsed;
    String   locationIP;
    DateTime creationDate;
    double   pr_val;
    double   cc_val;
    double   sp_val;
    double   bf_val;
  }; 

  // column id for Person
  enum Cid {
    ID, FistName, LastName,  Gender, Birthday, BrowserUsed,
    LocationIP, CreationDate, PR, CC, SP, BF, NumCol
  };
};

// Comment and Post are sub-classes of message
struct Comment {
  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID       id;
    String   browserUsed;
    DateTime creationDate;
    String   locationIP;
    Text     content;
    Uint     length;
  }; 

  // column id for Comment
  enum Cid {
    ID, BrowserUsed, CreationDate, LocationIP, Content, Length, 
    NumCol
  };
};

struct Post {
  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID       id;
    String   browserUsed;
    DateTime creationDate;
    String   locationIP;
    Text     content;
    Uint     length;
    String   language;
    String   imageFile;
    double   pr_val;
    double   cc_val;
    double   sp_val;
    double   bf_val;
  }; 

  // column id for Post
  enum Cid {
    ID, BrowserUsed, CreationDate, LocationIP, Content, Length,
    Language, ImageFile, PR, CC, SP, BF,
    NumCol
  };
};

struct Forum {
  struct PACKED_ATTR key {
    ID id;
  };
  
  struct PACKED_ATTR value {
    ID         id;
    LongString title;
    DateTime   creationDate;
  }; 

  // column id for Person
  enum Cid {
    ID, Title, CreationDate,
    NumCol
  };
};

/************* Relation (w/ property) ***************/
struct EdgeProp {
  ID src_key;
  ID dst_key;
  char prop[0];

  static void fill_str(ID s, ID d, const std::string &p, std::string &str) {
    EdgeProp *ep = reinterpret_cast<EdgeProp *>(&str[0]);
    ep->fill(s, d, p);
    assert(str.size() == meta_sz + p.size());
  }

  static uint64_t total_size(uint64_t prop_size) {
    return (meta_sz + prop_size);
  }

  static uint64_t prop_size(uint64_t total_size) {
    return (total_size - meta_sz);
  }

 private:
  void fill(ID s, ID d, const std::string &p) {
    src_key = s;
    dst_key = d;
    memcpy(prop, p.data(), p.size());
  }

  static const uint64_t meta_sz = 2 * sizeof(ID); 
  // std::string prop;

  // EdgeProp(ID s, ID d, std::string p)
  //   : src_key(s), dst_key(d), prop(p) { }
};

struct ForumHasMember {
  ID forumID;
  ID personID;
  DateTime joinDate;
};

struct Knows {
  ID person1id;
  ID person2id;
  DateTime creationDate;
};

struct PersonLikesComment {
  ID personID;
  ID commentID;
  DateTime creationDate;
};

struct PersonLikesPost {
  ID personID;
  ID postID;
  DateTime creationDate;
};

struct PersonStudyAt {
  ID personID;
  ID organisationID;
  Uint classYear;
};

struct PersonWorkAt {
  ID personID;
  ID organisationID;
  Uint workFrom;
};

}  // namespace ldbc
}  // namespace oltp
}  // namespace noccx
