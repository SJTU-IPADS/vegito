#include <fstream>
#include <algorithm>
#include "utils/timer.h"
#include "wiki_loader.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

WikiLoader::WikiLoader(std::string dataset_dir, int file_num) 
    : dataset_dir_(dataset_dir), file_num_(file_num) {
}

void WikiLoader::load_wiki_seg_graph(std::shared_ptr<SegGraph> seg_graph) {
    nocc::util::Breakdown_Timer timer;
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    vertex_t min = UINT64_MAX;
    vertex_t max = 0;
    for(int i = 0; i < file_num_; i++) {
        std::cout << "Load edges from: " << dataset_dir_ + std::to_string(i) << std::endl;
        std::ifstream ifile(dataset_dir_ + std::to_string(i));

        vertex_t s;
        vertex_t o;
        while (ifile >> s >> o) {
            min = s < min ? s : min;
            // min = o < min ? o : min;
            max = s > max ? s : max;
            // max = o > max ? o : max;
            edges.emplace_back(s, o);
        }
    }

    vertex_t max_vid = max - min;
    std::cout << "Max vertex id: " << max_vid << std::endl;

    // auto txn = seg_graph->begin_batch_loader();
    auto txn = seg_graph->create_graph_writer(0);
    for(vertex_t i = 0; i <= max_vid; i++) {
        auto src = txn.new_vertex();
        txn.put_vertex(i, "");
    }

    for (size_t i = 0; i < edges.size(); i++) {
        (&edges[i])->first -= min;
        (&edges[i])->second -= min;
    }

    std::vector<size_t> num_edges(max_vid + 1, 0);
    for (const auto &p : edges) {
        num_edges[p.first]++;
    }

    size_t cur_off = 0;
    std::vector<vertex_t> vertices_vec;
    std::vector<std::pair<vertex_t, vertex_t>> edges_vec;
    // std::vector<uint64_t> lats;

    for (vertex_t i = 0; i <= max_vid; i++) {
        vertices_vec.push_back(cur_off);
        cur_off += num_edges[i];
    }

    edges_vec.resize(cur_off);
    // lats.resize(cur_off);

    for (const auto &p : edges) {
        num_edges[p.first]--;
        edges_vec[vertices_vec[p.first] + num_edges[p.first]].first = p.first;
        edges_vec[vertices_vec[p.first] + num_edges[p.first]].second = p.second;
    }

    std::cout << "Start putting edges..." << std::endl;
    timer.start();

    // uint64_t idx = 0;
    for (const auto &p : edges_vec)
    {
        // uint64_t start = rdtsc();
        txn.put_edge(p.first, 0, p.second, "");
        // if (idx < cur_off) {
        //     lats[idx++] = rdtsc() - start;
        // }
    }

    float used_time = timer.get_diff_ms() / 1000;

    std::cout << "Start merging" << std::endl;
    txn.merge_segments(0);

    // std::sort(lats.begin(), lats.end());
    // double cdf[27] = {0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6,
    //                 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.96, 0.97, 0.98, 0.99, 0.999, 0.9999, 0.99999, 0.999999};

    // for (int i = 0; i < 27; i++) {
    //     std::cout << cdf[i] << " latency: " << lats[(int)(cdf[i] * cur_off)] << std::endl;
    // }


    std::cout << "[Seg-CSR] Finish loading edges,"
              << " edge num:" << edges_vec.size() << "," 
              << " time:" << used_time << " seconds," 
              << " thpt:" << edges.size() / used_time << " edges/second" 
              << std::endl;
}

void WikiLoader::load_wiki_seg_graph_random(std::shared_ptr<SegGraph> seg_graph) {
    nocc::util::Breakdown_Timer timer;
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    vertex_t min = UINT64_MAX;
    vertex_t max = 0;
    for(int i = 0; i < file_num_; i++) {
        std::cout << "Load edges from: " << dataset_dir_ + std::to_string(i) << std::endl;
        std::ifstream ifile(dataset_dir_ + std::to_string(i));

        vertex_t s;
        vertex_t o;
        while (ifile >> s >> o) {
            min = s < min ? s : min;
            // min = o < min ? o : min;
            max = s > max ? s : max;
            // max = o > max ? o : max;
            edges.emplace_back(s, o);
        }
    }

    vertex_t max_vid = max - min;
    std::cout << "Max vertex id: " << max_vid << std::endl;

    // auto txn = seg_graph->begin_batch_loader();
    auto txn = seg_graph->create_graph_writer(0);
    for(vertex_t i = 0; i <= max_vid; i++) {
        auto src = txn.new_vertex();
        txn.put_vertex(i, "");
    }

    std::cout << "Start shuffling..." << std::endl;
    std::random_shuffle(edges.begin(),edges.end());

    std::cout << "Start putting edges..." << std::endl;
    timer.start();

    for (const auto &p : edges)
    {
        txn.put_edge(p.first - min, 0, p.second - min, "");
    }

    float used_time = timer.get_diff_ms() / 1000;

    std::cout << "Start merging" << std::endl;
    txn.merge_segments(0);


    std::cout << "[Seg-CSR] Finish loading edges,"
              << " edge num:" << edges.size() << " ," 
              << " time:" << used_time << " seconds," 
              << " thpt:" << edges.size() / used_time << " edges/second" 
              << std::endl;
}

void WikiLoader::load_wiki_live_graph(std::shared_ptr<Graph> live_graph) {
    nocc::util::Breakdown_Timer timer;
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    vertex_t min = UINT64_MAX;
    vertex_t max = 0;
    for(int i = 0; i < file_num_; i++) {
        std::cout << "Load edges from: " << dataset_dir_ + std::to_string(i) << std::endl;
        std::ifstream ifile(dataset_dir_ + std::to_string(i));

        vertex_t s;
        vertex_t o;
        while (ifile >> s >> o) {
            min = s < min ? s : min;
            // min = o < min ? o : min;
            max = s > max ? s : max;
            // max = o > max ? o : max;
            edges.emplace_back(s, o);
        }
    }

    vertex_t max_vid = max - min;
    std::cout << "Max vertex id: " << max_vid << std::endl;

    auto txn = live_graph->begin_batch_loader();
    for(vertex_t i = 0; i <= max_vid; i++) {
        auto src = txn.new_vertex();
        txn.put_vertex(i, "");
    }

    for (size_t i = 0; i < edges.size(); i++) {
        (&edges[i])->first -= min;
        (&edges[i])->second -= min;
    }

    std::vector<size_t> num_edges(max_vid + 1, 0);
    for (const auto &p : edges) {
        num_edges[p.first]++;
    }

    size_t cur_off = 0;
    std::vector<vertex_t> vertices_vec;
    std::vector<std::pair<vertex_t, vertex_t>> edges_vec;

    for (vertex_t i = 0; i <= max_vid; i++) {
        vertices_vec.push_back(cur_off);
        cur_off += num_edges[i];
    }

    edges_vec.resize(cur_off);

    for (const auto &p : edges) {
        num_edges[p.first]--;
        edges_vec[vertices_vec[p.first] + num_edges[p.first]].first = p.first;
        edges_vec[vertices_vec[p.first] + num_edges[p.first]].second = p.second;
    }

    std::cout << "Start putting edges..." << std::endl;
    timer.start();

    for (const auto &p : edges_vec)
    {
        txn.put_edge(p.first, 0, p.second, "");
    }

    float used_time = timer.get_diff_ms() / 1000;

    std::cout << "[LiveGraph] Finish loading edges,"
              << " edge num:" << edges.size() << " ," 
              << " time:" << used_time << " seconds," 
              << " thpt:" << edges.size() / used_time << " edges/second" 
              << std::endl;
}

void WikiLoader::load_wiki_live_graph_random(std::shared_ptr<Graph> live_graph) {
    nocc::util::Breakdown_Timer timer;
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    vertex_t min = UINT64_MAX;
    vertex_t max = 0;
    for(int i = 0; i < file_num_; i++) {
        std::cout << "Load edges from: " << dataset_dir_ + std::to_string(i) << std::endl;
        std::ifstream ifile(dataset_dir_ + std::to_string(i));

        vertex_t s;
        vertex_t o;
        while (ifile >> s >> o) {
            min = s < min ? s : min;
            // min = o < min ? o : min;
            max = s > max ? s : max;
            // max = o > max ? o : max;
            edges.emplace_back(s, o);
        }
    }

    vertex_t max_vid = max - min;
    std::cout << "Max vertex id: " << max_vid << std::endl;

    auto txn = live_graph->begin_batch_loader();
    for(vertex_t i = 0; i <= max_vid; i++) {
        auto src = txn.new_vertex();
        txn.put_vertex(i, "");
    }

    std::cout << "Start shuffling..." << std::endl;
    std::random_shuffle(edges.begin(),edges.end());

    std::cout << "Start putting edges..." << std::endl;
    timer.start();

    for (const auto &p : edges)
    {
        txn.put_edge(p.first - min, 0, p.second - min, "");
    }

    float used_time = timer.get_diff_ms() / 1000;

    std::cout << "[LiveGraph] Finish loading edges,"
              << " edge num:" << edges.size() << " ," 
              << " time:" << used_time << " seconds," 
              << " thpt:" << edges.size() / used_time << " edges/second" 
              << std::endl;
}

void WikiLoader::load_wiki_csr_graph(std::shared_ptr<CSRGraph> csr_graph, bool random) {
    nocc::util::Breakdown_Timer timer;
    timer.start();
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    
    csr_graph->vertices.resize(1);
    csr_graph->edges.resize(1);

    vertex_t min = UINT64_MAX;
    vertex_t max = 0;
    for(int i = 0; i < file_num_; i++) {
        std::cout << "Load edges from: " << dataset_dir_ + std::to_string(i) << std::endl;
        std::ifstream ifile(dataset_dir_ + std::to_string(i));

        vertex_t s;
        vertex_t o;
        while (ifile >> s >> o) {
            min = s < min ? s : min;
            // min = o < min ? o : min;
            max = s > max ? s : max;
            // max = o > max ? o : max;
            edges.emplace_back(s, o);
        }
    }

    vertex_t max_vid = max - min;
    std::cout << "Max vertex id: " << max_vid << std::endl;

    for (size_t i = 0; i < edges.size(); i++) {
        (&edges[i])->first -= min;
        (&edges[i])->second -= min;
    }

    if (random) {
        std::cout << "Start shuffling..." << std::endl;
        std::random_shuffle(edges.begin(),edges.end());
    }

    std::vector<size_t> num_edges(max_vid + 1, 0);
    for (const auto &p : edges) {
        num_edges[p.first]++;
    }

    off_t cur_off = 0;
    for (vertex_t i = 0; i <= max_vid; i++) {
        csr_graph->vertices[0].push_back(cur_off);
        cur_off += num_edges[i];
    }

    csr_graph->edges[0].reserve(cur_off);

    for (const auto &p : edges) {
        num_edges[p.first]--;
        EdgeEntry entry;
        entry.set_dst(p.second);
        csr_graph->edges[0][csr_graph->vertices[0][p.first] + num_edges[p.first]] = entry;
    }

    std::cout << "[CSR] Finish loading edges, "
              << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;
}
}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
