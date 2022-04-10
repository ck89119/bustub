//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page->is_dirty_) {
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (auto it : page_table_) {
    FlushPgImp(it.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = AvailableFrame();
  if (frame_id == NO_AVAILABLE_FRAME_ID) {
    return nullptr;
  }

  *page_id = AllocatePage();
  Page *page = pages_ + frame_id;
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->WritePage(*page_id, page->data_);
  // update page_table
  page_table_[*page_id] = frame_id;
  assert(page_table_.size() <= pool_size_);
  replacer_->Pin(frame_id);
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock<std::mutex> lock(latch_);

  // if page in pages
  if (page_table_.find(page_id) != page_table_.end()) {
    // LOG_DEBUG("page_id: %d in page_table_", page_id);
    frame_id_t frame_id = page_table_[page_id];
    Page *page = pages_ + frame_id;
    page->pin_count_ += 1;
    replacer_->Pin(frame_id);
    return page;
  }

  // get frame
  frame_id_t frame_id = AvailableFrame();
  if (frame_id == NO_AVAILABLE_FRAME_ID) {
    return nullptr;
  }

  // get page
  Page *page = pages_ + frame_id;
  // read from disk
  disk_manager_->ReadPage(page_id, page->GetData());
  // set new page_id
  page->page_id_ = page_id;
  // inc pin count
  page->pin_count_ = 1;
  // not dirty
  page->is_dirty_ = false;
  // update page_table
  page_table_[page_id] = frame_id;
  assert(page_table_.size() <= pool_size_);
  replacer_->Pin(frame_id);
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    DeallocatePage(page_id);
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  // will delete immediately, so not need to write back dirty content
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock(latch_);

  // deleted already
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  page->is_dirty_ |= is_dirty;
  bool result = page->pin_count_ > 0;
  if (page->pin_count_ > 0) {
    page->pin_count_ -= 1;
  }

  // LOG_INFO("pin_count = %d", page->pin_count_);
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return result;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

frame_id_t BufferPoolManagerInstance::AvailableFrame() {
  // LOG_DEBUG("free_list_.size() = %lu", free_list_.size());
  // LOG_DEBUG("replacer_->Size() = %lu", replacer_->Size());
  if (free_list_.empty() && replacer_->Size() == 0) {
    // LOG_INFO("NO_AVAILABLE_FRAME");
    return NO_AVAILABLE_FRAME_ID;
  }

  frame_id_t frame_id;
  // if no free node, get one from replacer
  if (free_list_.empty()) {
    replacer_->Victim(&frame_id);
    Page *page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page_table_.erase(page->page_id_);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // LOG_INFO("AVAILABLE_FRAME, id = %d", frame_id);
  return frame_id;
}
}  // namespace bustub
