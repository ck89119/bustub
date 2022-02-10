//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

#include "common/logger.h"
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock lock(mutex_);

  if (table_.empty()) {
    return false;
  }

  *frame_id = list_.front();
  list_.pop_front();
  table_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock lock(mutex_);

  if (table_.find(frame_id) != table_.end()) {
    auto it = table_[frame_id];
    list_.erase(it);
    table_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock lock(mutex_);

  LOG_INFO("LRUReplacer::Unpin, frame_id = %d", frame_id);
  if (table_.find(frame_id) == table_.end()) {
    LOG_INFO("LRUReplacer::Unpin, frame_id = %d not in table", frame_id);
    list_.push_back(frame_id);
    table_[frame_id] = --list_.end();
  }
}

size_t LRUReplacer::Size() {
  std::unique_lock lock(mutex_);
  return table_.size();
}

}  // namespace bustub
