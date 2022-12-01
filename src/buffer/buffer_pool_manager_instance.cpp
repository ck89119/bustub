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

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
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
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // if page in pages
  if (page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("page_id: %d in page_table_", page_id);
    Page *page = pages_ + frame_id;
    page->pin_count_ += 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // get available frame
  frame_id = AvailableFrame();
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
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  // deleted already
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = pages_ + frame_id;
  // pin_count <= 0
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;
  page->pin_count_ -= 1;
  // LOG_INFO("pin_count = %d", page->pin_count_);
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  FlushPg(pages_ + frame_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    std::unique_lock<std::mutex> lock(latch_);
    FlushPg(pages_ + i);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    DeallocatePage(page_id);
    return true;
  }

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
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::AvailableFrame() -> frame_id_t {
  // LOG_DEBUG("free_list_.size() = %lu", free_list_.size());
  // LOG_DEBUG("replacer_->Size() = %lu", replacer_->Size());
  if (free_list_.empty() && replacer_->Size() == 0) {
    // LOG_INFO("NO_AVAILABLE_FRAME");
    return NO_AVAILABLE_FRAME_ID;
  }

  frame_id_t frame_id;
  // if no free node, get one from replacer
  if (free_list_.empty()) {
    replacer_->Evict(&frame_id);
    Page *page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page_table_->Remove(page->page_id_);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // LOG_INFO("AVAILABLE_FRAME, id = %d", frame_id);
  return frame_id;
}

void BufferPoolManagerInstance::FlushPg(Page *page) {
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
}

}  // namespace bustub
