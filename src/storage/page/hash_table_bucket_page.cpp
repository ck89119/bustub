//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }

    if (cmp(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t i;
  if (IsFull()) {
    return false;
  }

  for (i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
      return false;
    }
  }

  for (i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      continue;
    }

    array_[i] = std::make_pair(key, value);
    SetReadable(i);
    return true;
  }
  assert(i < BUCKET_ARRAY_SIZE);

  array_[i] = std::make_pair(key, value);
  SetOccupied(i);
  SetReadable(i);

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (IsReadable(i) && cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetZero(bucket_idx, const_cast<char *>(readable_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return GetBit(bucket_idx, const_cast<char *>(occupied_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  SetOne(bucket_idx, const_cast<char *>(occupied_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return GetBit(bucket_idx, const_cast<char *>(readable_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  SetOne(bucket_idx, const_cast<char *>(readable_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return std::get<1>(GetStatistics());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  auto [size, taken, free] = GetStatistics();
  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<uint32_t, uint32_t> HASH_TABLE_BUCKET_TYPE::GetBitLocation(uint32_t bucket_idx) const {
  uint32_t index = bucket_idx / 8;
  uint32_t bit_offset = bucket_idx % 8;
  return {index, bit_offset};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetBit(uint32_t bucket_idx, const char *arr) const {
  auto [index, bit_offset] = GetBitLocation(bucket_idx);
  return (arr[index] >> bit_offset) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetZero(uint32_t bucket_idx, char *arr) {
  auto [index, bit_offset] = GetBitLocation(bucket_idx);
  *(arr + index) &= (0xff - (1 << bit_offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOne(uint32_t bucket_idx, char *arr) {
  auto [index, bit_offset] = GetBitLocation(bucket_idx);
  *(arr + index) |= (1 << bit_offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::tuple<uint32_t, uint32_t, uint32_t> HASH_TABLE_BUCKET_TYPE::GetStatistics() const {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }
  return {size, taken, free};
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
