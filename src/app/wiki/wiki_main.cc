#include "wiki_main.h"
#include "wiki_runner.h"
#include "wiki_config.h"
#include "framework/framework_cfg.h"
#include <cstdio>

namespace nocc {
namespace oltp {
namespace wiki {

volatile bool cluster_running;

void WikiTest(int argc, char **argv) {
  /* Init global config */
  wikiConfig.parse_wiki_xml(nocc::framework::config.getConfigFile());
  wikiConfig.print();

  wiki::WikiRunner runner(wikiConfig);
  runner.run();
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
