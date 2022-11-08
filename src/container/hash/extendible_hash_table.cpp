//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  dir_page->IncrGlobalDepth();

  page_id_t page_id_0;
  buffer_pool_manager_->NewPage(&page_id_0);
  dir_page->SetBucketPageId(0, page_id_0);
  dir_page->SetLocalDepth(0, 1);

  page_id_t page_id_1;
  buffer_pool_manager_->NewPage(&page_id_1);
  dir_page->SetBucketPageId(1, page_id_1);
  dir_page->SetLocalDepth(1, 1);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(page_id_0, false, nullptr);
  buffer_pool_manager_->UnpinPage(page_id_1, false, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->RLatch();
  bool status = bucket_page->GetValue(key, comparator_, result);
  // page->RUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  // table_latch_.RUnlock();
  return status;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->RLatch();
  if (bucket_page->IsFull()) {
    // page->RUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    // table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }
  // page->RUnlatch();

  bool status = false;
  // page->WLatch();
  if (!bucket_page->IsFull()) {
    status = bucket_page->Insert(key, value, comparator_);
  }
  // page->WUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  // table_latch_.RUnlock();
  return status;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // bucket_page->RLatch();
  // if (!bucket_page->IsFull()) {
  // }

  // double size the bucket array
  uint32_t old_size = dir_page->Size();
  dir_page->IncrGlobalDepth();
  for (uint32_t i = 0; i < old_size; ++i) {
    dir_page->SetBucketPageId(i + old_size, dir_page->GetBucketPageId(i));
  }

  // new page
  uint32_t split_image_idx = bucket_idx + old_size;
  page_id_t split_image_page_id;
  HASH_TABLE_BUCKET_TYPE *split_image_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_image_page_id));

  dir_page->SetBucketPageId(split_image_idx, split_image_page_id);
  dir_page->IncrLocalDepth(bucket_idx);
  dir_page->IncrLocalDepth(split_image_idx);

  // move key/value whose Hash(key)'s highest bit is 1 to new split page
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    uint32_t new_idx = KeyToDirectoryIndex(bucket_page->KeyAt(i), dir_page);
    if (new_idx != bucket_idx) {
      split_image_page->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_);
      bucket_page->RemoveAt(i);
    }
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(split_image_page_id, true, nullptr);

  // retry insert
  bucket_idx = KeyToDirectoryIndex(key, dir_page);
  bucket_page_id = KeyToPageId(key, dir_page);
  bucket_page = FetchBucketPage(bucket_page_id);

  bool status = bucket_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  return status;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->WLatch();
  bool status = bucket_page->Remove(key, value, comparator_);
  // page->WUnlatch();

  // page->RLatch();
  if (bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    Merge(transaction, key, value);
  }
  // page->RUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  // table_latch_.RUnlock();
  return status;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // have acquired bucket_page read latch, no need to acquire again

  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
  uint32_t split_image_idx = (bucket_idx + dir_page->Size() / 2) % dir_page->Size();
  uint32_t split_image_depth = dir_page->GetLocalDepth(split_image_idx);

  // 3 scenarios no need to need to merge:
  // 1. no longer empty
  // 2. local_depth = 0
  // 3. local_depth != split_image_depth
  if (bucket_page->IsEmpty() && local_depth != 0 && split_image_depth == local_depth) {
    dir_page->SetBucketPageId(bucket_idx, split_image_idx);
    // unpin first, then delete
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->DeletePage(bucket_page_id);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  } else {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  }
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
