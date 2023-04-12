//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <deque>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  auto GetRootPage() -> BPlusTreePage *;

  auto FindLeafPageForRead(const KeyType &key, bool write_latch_leaf = false) -> LeafPage *;

  auto FindLeafPageForWrite(const KeyType &key, WriteType write_type) -> std::deque<BPlusTreePage *>;

  auto GetParent(BPlusTreePage *tree_page, bool create_if_not_exist) -> InternalPage *;

  void UpdateParentPageId(const page_id_t &child_page_id, const page_id_t &parent_page_id);

  auto LeafInsert(std::deque<BPlusTreePage *> &latched_pages, const KeyType &key, const ValueType &value) -> bool;

  void LeafSplit(std::deque<BPlusTreePage *> &latched_pages);

  void InternalInsert(std::deque<BPlusTreePage *> &latched_pages, InternalPage *internal_page, const KeyType &key,
                      const page_id_t &value);

  void InternalSplit(std::deque<BPlusTreePage *> &latched_pages, InternalPage *internal_page, const KeyType &key,
                     const page_id_t &value);

  void LeafMerge(LeafPage *leaf);

  void LeafMerge(LeafPage *left, LeafPage *right);

  auto BorrowFromSiblingLeaf(LeafPage *leaf) -> bool;

  void InternalMerge(InternalPage *internal_page);

  void InternalMerge(InternalPage *left, InternalPage *right);

  auto BorrowFromSiblingInternal(InternalPage *internal_page) -> bool;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  // protect root_page_id_
  ReaderWriterLatch tree_latch_;
};

}  // namespace bustub
