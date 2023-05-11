#include <getopt.h>
#include "wiki_config.h"

// for parsing xml
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

using namespace std;

namespace nocc {
namespace oltp {
namespace wiki {
WikiConfig wikiConfig;

WikiConfig::WikiConfig() {}

void WikiConfig::parse_args(int argc, char **argv) {
    static struct option long_options[] =
    {
      {"update-nthreads"    , required_argument , 0, 'u'},
      {"query-nthreads"     , required_argument , 0, 'q'},
      {"analytics-nthreads" , required_argument , 0, 'a'},
      {"graph-dir"          , required_argument , 0, 'g'},
      {0, 0, 0, 0}
    };
    const char *optstring = "u:q:a:";

    while (1) {    
        int option_index = 0;
        int opt = getopt_long(argc, argv, optstring, long_options, &option_index);
        if (opt == -1) break;

        switch (opt) {
        case 0:
            if (long_options[option_index].flag != 0)
                break;
            abort();
        case 'u':
            update_nthreads_ = strtoul(optarg, NULL, 10);
            break;
        case 'a':
            analytics_nthreads_ = strtoul(optarg, NULL, 10);
            break;
        case 'q':
            query_nthreads_ = strtoul(optarg, NULL, 10);
            break;
        case '?':
            /* getopt_long already printed an error message. */
        default:
            fprintf(stdout,"Invalid command line val: %s\n", argv[optind - 1]);
            abort();
        }
    }
    optind = 1;  // important: reset optind
}

void WikiConfig::parse_wiki_xml(const std::string &config_file) {
  // test configuration file
  if (config_file == "") {
    printf("[Wiki Config] No configure xml file\n");
    return;
  } else {
    ifstream ifs;
    ifs.open(config_file);
    if (!ifs) {
      printf("[Wiki Config] Cannot open configure file: %s\n", 
             config_file.c_str());
      return;
    } 
  }
  printf("[Wiki Config] Use configure file: %s\n", config_file.c_str());

  // parse input xml
  using namespace boost::property_tree;
  ptree pt;

  try {
    read_xml(config_file, pt);

    graph_type_ = pt.get<string> ("bench.wiki.graph_type");
    dataset_dir_ = pt.get<string> ("bench.wiki.dataset_dir");
    file_num_ = pt.get<int> ("bench.wiki.file_num");
    random_ = pt.get<bool> ("bench.wiki.random");
    scan_nthreads_ = pt.get<int> ("bench.scan_threads");
    run_sec_ = pt.get<int> ("bench.run_sec");

  } catch (const ptree_error &e) {
  
  }
}

void WikiConfig::print() const {
    std::cout << "System Configuration:" << std::endl;
    std::cout << "  1. Time" << std::endl;
    std::cout << "    1) Run time sec: " << run_sec_ << std::endl;
    std::cout << "  2. Thread config" << std::endl;
    std::cout << "    1) Update thread num: " << update_nthreads_ << std::endl;
    std::cout << "    2) Analytics thread num: " << analytics_nthreads_ << std::endl;
    std::cout << "    3) Query thread num: " << query_nthreads_ << std::endl;
    std::cout << "    3) Scan thread num: " << scan_nthreads_ << std::endl;
    std::cout << "  3. Graph config" << std::endl;
    std::cout << "    1) Graph type: " << graph_type_ << std::endl;
    std::cout << "    2) Dataset load: " << (random_ ? "random insertion" : "bulk load") << std::endl;
    std::cout << "    3) Dataset directory: " << dataset_dir_ << std::endl;
    std::cout << "    3) Data file number: " << file_num_ << std::endl;
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
