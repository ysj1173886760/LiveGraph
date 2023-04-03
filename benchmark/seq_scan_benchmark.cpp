/**
 * @file seq_scan_benchmark.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-04-01
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <exception>
#include <limits>
#include <ostream>
#include <random>
#include <gflags/gflags.h>
#include "core/graph.hpp"
#include "core/livegraph.hpp"
#include "core/transaction.hpp"
#include <iostream>

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

static const std::string db_name = "seq_scan_benchmark";
const int64_t point_per_thread = 100;
const int64_t edge_per_point = 100;
const int thread_num = 4;
bool wait_visible = true;
std::atomic_int64_t qps[thread_num];

void Work(livegraph::Graph *db, int idx) {
  auto txn = db->begin_read_only_transaction();
  volatile int64_t id;
  while (true) {
    for (int i = point_per_thread * idx; i < point_per_thread * (idx + 1); i++) {
      auto iter = txn.get_edges(i, 0);
      while (iter.valid()) {
        // id = iter.dst_id();
        iter.next();
      }
      qps[idx].fetch_add(1);
    }
  }
}

void PrepareEdge(livegraph::Graph *db) {
  auto value = "arcane";
  auto txn = db->begin_batch_loader();
  try {
    for (int i = 0; i < point_per_thread * thread_num; i++) {
      txn.new_vertex();
    }
    for (int i = 0; i < point_per_thread * thread_num; i++) {
      for (int j = 0; j < edge_per_point; j++) {
        txn.put_edge(i, 0, j, value);
      }
    }
  } catch (const std::exception &e) {
    std::cout << e.what() << std::endl;
  }
  txn.commit();
}

int main(int argc, char* argv[]) {
  const std::string block_path = "./livegraph_block";
  const std::string wal_path = "./livegraph_wal";
  livegraph::Graph graph(block_path, wal_path);
  std::vector<std::thread> workers;
  PrepareEdge(&graph);
  for (int i = 0; i < thread_num; i++) {
    workers.push_back(std::thread([&](int idx) {
      Work(&graph, idx);
    }, i));
  }
  auto thread = std::thread([&]() {
    int64_t last = 0;
    while (true) {
      usleep(1000000);
      int64_t total = 0;
      for (int i = 0; i < thread_num; i++) {
        total += qps[i];
      }
      std::cout << "qps: " << total - last << std::endl;
      last = total;
    }
  });
  thread.join();
  return 0;
}