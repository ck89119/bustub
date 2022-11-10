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
  for (int i = 0; i < DIRECTORY_ARRAY_SIZE; ++i) {
    dir_page->SetLocalDepth(i, 0);
  }

  page_id_t page_id;
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(0, page_id);

  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
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
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->RLatch();
  bool status = bucket_page->GetValue(key, comparator_, result);
  // page->RUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  // table_latch_.RUnlock();
  table_latch_.WUnlock();
  return status;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.RLock();
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->WLatch();
  if (bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    // page->WUnlatch();
    // table_latch_.RUnlock();
    table_latch_.WUnlock();
    return SplitInsert(transaction, key, value);
  }

  bool status = bucket_page->Insert(key, value, comparator_);
  // page->WUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  // table_latch_.RUnlock();
  table_latch_.WUnlock();
  return status;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // if bucket is not full, insert directly
  if (!bucket_page->IsFull()) {
    bool status = bucket_page->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    table_latch_.WUnlock();
    return status;
  }

  // if bucket is full
  // if no split image, need to double size dir array first
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    // init split images
    for (uint32_t i = 0; i < dir_page->Size(); ++i) {
      dir_page->SetBucketPageId(i + dir_page->Size(), dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(i + dir_page->Size(), dir_page->GetLocalDepth(i));
    }
    dir_page->IncrGlobalDepth();
  }

  // current bucket is full, move some element to split image
  // new page
  page_id_t split_image_page_id;
  HASH_TABLE_BUCKET_TYPE *split_image_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_image_page_id));

  // change meta info point to bucket_page previously
  uint32_t idx = bucket_idx;
  uint32_t inc = dir_page->GetLocalHighBit(bucket_idx);
  uint32_t old_local_depth = dir_page->GetLocalDepth(bucket_idx);
  do {
    assert(dir_page->GetLocalDepth(idx) == old_local_depth);

    // update local depth
    dir_page->IncrLocalDepth(idx);
    idx = (idx + inc) % dir_page->Size();
  } while (idx != bucket_idx);

  inc = dir_page->GetLocalHighBit(bucket_idx);
  do {
    assert(dir_page->GetBucketPageId(idx) == bucket_page_id);

    // update bucket page id
    dir_page->SetBucketPageId(idx, split_image_page_id);
    idx = (idx + inc) % dir_page->Size();
  } while (idx != bucket_idx);

  // move key/value to new split page
  uint32_t local_mask = dir_page->GetLocalDepthMask(bucket_idx);
  uint32_t new_bucket_idx = bucket_idx & local_mask;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    uint32_t new_idx = Hash(bucket_page->KeyAt(i)) & local_mask;
    if (new_idx == new_bucket_idx) {
      split_image_page->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_);
      bucket_page->RemoveAt(i);
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(split_image_page_id, true, nullptr);
  table_latch_.WUnlock();
  return SplitInsert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.RLock();
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // Page *page = reinterpret_cast<Page *>(bucket_page);

  // page->WLatch();
  bool status = bucket_page->Remove(key, value, comparator_);

  // return if remove unsuccess
  if (!status) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    // page->WUnlatch();
    // table_latch_.RUnlock();
    table_latch_.WUnlock();
    return status;
  }

  if (bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    // page->WUnlatch();
    // table_latch_.RUnlock();
    table_latch_.WUnlock();
    Merge(transaction, key, value);
    return status;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  // page->WUnlatch();
  // table_latch_.RUnlock();
  table_latch_.WUnlock();
  return status;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // have acquired bucket_page read latch, no need to acquire again

  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
  uint32_t split_image_idx = (bucket_idx + dir_page->GetLocalHighBit(bucket_idx) / 2) % dir_page->Size();
  uint32_t split_image_depth = dir_page->GetLocalDepth(split_image_idx);

  // 3 scenarios no need to merge:
  // 1. no longer empty
  // 2. local_depth = 0
  // 3. local_depth != split_image_depth
  if (bucket_page->IsEmpty() && local_depth != 0 && split_image_depth == local_depth) {
    uint32_t idx = bucket_idx;
    uint32_t inc = dir_page->GetLocalHighBit(bucket_idx) / 2;
    do {
      assert(dir_page->GetLocalDepth(idx) == local_depth);

      dir_page->SetBucketPageId(idx, dir_page->GetBucketPageId(split_image_idx));
      dir_page->DecrLocalDepth(idx);
      idx = (idx + inc) % dir_page->Size();
    } while (idx != bucket_idx);

    // unpin first, then delete
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->DeletePage(bucket_page_id);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  } else {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  }

  table_latch_.WUnlock();
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
