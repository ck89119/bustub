//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  ref_ = std::vector<bool>(num_pages);
  pinned_ = std::vector<bool>(num_pages, true);
  cur_pos_ = 0;
  size_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (size_ == 0) {
    return false;
  }

  for (; ; cur_pos_ = (cur_pos_ + 1) % ref_.size()) {
    if (pinned_[cur_pos_]) {
      continue;
    }

    if (!ref_[cur_pos_]) {
      *frame_id = cur_pos_;
      cur_pos_ = (cur_pos_ + 1) % ref_.size();
      break;
    }

    ref_[cur_pos_] = false;
  }
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (!pinned_[frame_id]) {
    pinned_[frame_id] = true;
    --size_;
  }
  ref_[frame_id] = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (pinned_[frame_id]) {
    pinned_[frame_id] = false;
    ++size_;
  }
}

size_t ClockReplacer::Size() { 
  return size_;
}

}  // namespace bustub
