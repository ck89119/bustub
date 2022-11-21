//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <set>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  assert((size_t)dir_index < dir_.size());
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto status = dir_[index]->Remove(key);
  CheckIntegrity();
  return status;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);

  while (!dir_[index]->Insert(key, value)) {
    auto old_bucket = dir_[index];

    if (old_bucket->GetDepth() == global_depth_) {
      // double size dir_ array
      ++global_depth_;
      dir_.insert(dir_.end(), dir_.begin(), dir_.end());
      assert(1 << global_depth_ == dir_.size());

      index = IndexOf(key);
    }

    // new bucket
    old_bucket->IncrementDepth();
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, old_bucket->GetDepth());
    ++num_buckets_;

    // modify dir_
    auto i = index;
    do {
      dir_[i] = new_bucket;
      i = (i + (1 << old_bucket->GetDepth())) % dir_.size();
    } while (i != index);

    // move data to new bucket
    auto local_mask = (1 << old_bucket->GetDepth()) - 1;
    auto suffix = local_mask & index;
    auto items = old_bucket->GetItems();
    for (auto &[k, v] : items) {
      if (suffix == (std::hash<K>()(k) & local_mask)) {
        new_bucket->Insert(k, v);
        old_bucket->Remove(k);
      }
    }
  }

  CheckIntegrity();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::CheckIntegrity() -> void {
  assert(1 << global_depth_ == dir_.size());

  for (size_t i = 0; i < dir_.size(); ++i) {
    assert(dir_[i]->GetDepth() <= global_depth_);
  }

  std::vector<bool> visited(dir_.size());
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (visited[i]) {
      continue;
    }

    auto local_depth = dir_[i]->GetDepth();
    auto addr = dir_[i];
    for (auto j = i; j < dir_.size(); j += 1 << local_depth) {
      assert(!visited[j]);
      visited[j] = true;

      assert(addr == dir_[j]);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it = list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }

  if (!IsFull()) {
    list_.emplace_back(key, value);
    return true;
  }

  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
