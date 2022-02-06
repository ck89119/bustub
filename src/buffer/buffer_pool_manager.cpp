//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  
  // if page in pages
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    return pages_ + frame_id;    
  }

  // get frame
  frame_id_t frame_id = AvailableFrame();
  if (frame_id == -1) {
    return nullptr;
  }

  // get page
  Page* page = pages_ + frame_id;
  // set new page_id
  page->page_id_ = page_id;
  // read from disk
  disk_manager_->ReadPage(page_id, page->GetData());
  // update page_table
  page_table_[page_id] = frame_id;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;
  page->is_dirty_ = is_dirty;
  page->pin_count_ -= 1;

  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return page->pin_count_ == 0;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (replacer_->Size() == 0) {
    return nullptr;
  }

  *page_id = disk_manager_->AllocatePage();
  frame_id_t frame_id = AvailableFrame();
  // replacer_->Pin(frame_id);
  Page* page = pages_ + frame_id;
  page->page_id_ = *page_id;
  // update page_table
  page_table_[*page_id] = frame_id;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);
  page->ResetMemory();
  page_table_.erase(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (auto [page_id, _] : page_table_) {
    FlushPageImpl(page_id);
  }
}

frame_id_t BufferPoolManager::AvailableFrame() {
  frame_id_t frame_id = -1;
  // if no free node, get one from replacer
  if (free_list_.empty()) {
    if (replacer_->Size() == 0) {
      return frame_id;
    }

    replacer_->Victim(&frame_id);
    Page* page = pages_ + frame_id;
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page->ResetMemory();
    page_table_.erase(page->page_id_);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  return frame_id;
}

}  // namespace bustub
