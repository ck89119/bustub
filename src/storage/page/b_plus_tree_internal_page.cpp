//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &comparator) const -> int {
  //  for (int i = 1; i < GetSize(); ++i) {
  //    if (comparator(KeyAt(i), key) > 0) {
  //      return i;
  //    }
  //  }
  //  return GetSize();
  int l = 0;
  int r = GetSize();
  while (l + 1 < r) {
    auto m = (l + r) / 2;
    if (comparator(KeyAt(m), key) > 0) {
      r = m;
    } else {
      l = m;
    }
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKV(int index) const -> MappingType { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKV(int index, const MappingType &&kv) { array_[index] = kv; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKV(const KeyType &key, const ValueType &value,
                                              const KeyComparator &comparator) {
  auto index = UpperBound(key, comparator);
  // move to next location
  for (int i = GetSize(); i > index; --i) {
    SetKV(i, GetKV(i - 1));
  }
  SetKV(index, {key, value});
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfAndInsert(B_PLUS_TREE_INTERNAL_PAGE_TYPE *another_internal,
                                                       const KeyType &key, const ValueType &value,
                                                       const KeyComparator &comparator) {
  // move data [middle, max_size) to new node
  auto max_size = GetMaxSize();
  // make sure sizeof(right) == sizeof(left) or sizeof(left) + 1 after insert
  int middle = (max_size + 1) / 2;
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *page_to_insert = nullptr;
  if (UpperBound(key, comparator) >= middle) {
    page_to_insert = another_internal;
  } else {
    middle -= 1;
    page_to_insert = this;
  }

  //    for (int i = middle; i < max_size; ++i) {
  //      another_internal->SetKV(i - middle, GetKV(i));
  //    }
  std::copy(&array_[middle], &array_[GetSize()], another_internal->array_);

  // set size
  SetSize(middle);
  another_internal->SetSize(max_size - middle);

  // insert KV into page
  if (page_to_insert->GetPageId() == another_internal->GetPageId() && comparator(key, another_internal->KeyAt(0)) < 0) {
    for (int i = another_internal->GetSize(); i > 0; --i) {
      another_internal->SetKV(i, another_internal->GetKV(i - 1));
    }
    another_internal->SetKV(0, {key, value});
    another_internal->IncreaseSize(1);
  } else {
    page_to_insert->InsertKV(key, value, comparator);
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
