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

#include "ch_runner.h"
#include "ch_config.h"

namespace nocc {
namespace oltp {
namespace ch {

void ChTest(int argc, char **argv) {
  chConfig.parse_ch_args(argc, argv);
  chConfig.parse_ch_xml(config.getConfigFile());
  chConfig.printConfig();
  ChRunner().run();
  /* End CH bootstrap function */
}

}  // namespace ch
}  // namespace oltp
}  // namespace nocc
