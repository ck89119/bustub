//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  accesss_history_ = std::vector<std::list<size_t>>(num_frames);
  evictable_ = std::vector<bool>(num_frames);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);

  size_t current_max_diff = 0;
  size_t current_max_diff_timestamp = 0;
  bool status = false;

  for (size_t i = 0; i < replacer_size_; ++i) {
    if (accesss_history_[i].empty() || !evictable_[i]) {
      continue;
    }

    auto [diff, timestamp] = GetDiff(i);
    if (diff > current_max_diff || (diff == current_max_diff && timestamp < current_max_diff_timestamp)) {
      *frame_id = i;
      current_max_diff = diff;
      current_max_diff_timestamp = timestamp;
      status = true;
    }
  }

  if (status) {
    accesss_history_[*frame_id].clear();
  }

  return status;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::unique_lock<std::mutex> l(latch_);

  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "larger than replacer_size_");

  auto &frame_accesss_history = accesss_history_[frame_id];
  frame_accesss_history.push_back(current_timestamp_++);
  if (frame_accesss_history.size() > k_) {
    frame_accesss_history.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> l(latch_);

  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "larger than replacer_size_");

  evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> l(latch_);

  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "larger than replacer_size_");

  if (accesss_history_[frame_id].empty()) {
    return;
  }

  BUSTUB_ASSERT(evictable_[frame_id], "non-evictable frame");

  accesss_history_[frame_id].clear();
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> l(latch_);

  auto size = 0;
  for (size_t i = 0; i < replacer_size_; ++i) {
    if (!accesss_history_[i].empty() && evictable_[i]) {
      size += 1;
    }
  }
  return size;
}

auto LRUKReplacer::GetDiff(frame_id_t frame_id) -> std::pair<size_t, size_t> {
  auto recent_k_timestamp = accesss_history_[frame_id].front();
  if (accesss_history_[frame_id].size() < k_) {
    return std::make_pair(0xffffffffffffffffULL, recent_k_timestamp);
  }
  return std::make_pair(current_timestamp_ - recent_k_timestamp, recent_k_timestamp);
}

}  // namespace bustub
