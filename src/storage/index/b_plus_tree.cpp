#include "storage/index/b_plus_tree.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  LOG_INFO("buffer_pool_manager_.size() = %zu, leaf_max_size = %d, internal_max_size = %d",
           buffer_pool_manager->GetPoolSize(), leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::cout << "GetValue, key = " << key << std::endl;
  if (IsEmpty()) {
    return false;
  }

  auto *leaf = FindLeafPageForRead(key);
  auto index = leaf->LowerBound(key, comparator_);
  // key exists
  bool status = false;
  if (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) == 0) {
    result->push_back(leaf->ValueAt(index));
    status = true;
  }

  leaf->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return status;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // std::cout << "Insert, key = " << key << std::endl;
  if (IsEmpty()) {
    // acquire tree latch to update root_page_id_
    tree_latch_.WLock();
    if (IsEmpty()) {
      auto *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
      leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      leaf->SetNextPageId(INVALID_PAGE_ID);
      UpdateRootPageId(true);
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    }
    tree_latch_.WUnlock();
  }

  auto *leaf = FindLeafPageForRead(key, true, transaction);
  if (leaf->IsSafe(WriteType::INSERT)) {
    auto ans = LeafInsert(key, value, transaction);
    ReleaseAllLatches(transaction, ans);
    return ans;
  }

  // if not safe, release latches first
  ReleaseAllLatches(transaction, false);
  FindLeafPageForWrite(key, WriteType::INSERT, transaction);
  auto ans = LeafInsert(key, value, transaction);
  ReleaseAllLatches(transaction, ans);
  return ans;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "Remove, key = " << key << std::endl;
  if (IsEmpty()) {
    return;
  }

  auto *leaf = FindLeafPageForRead(key, true, transaction);
  if (leaf->IsSafe(WriteType::DELETE)) {
    auto ans = LeafRemove(key, transaction);
    ReleaseAllLatches(transaction, ans);
    return;
  }

  // if not safe, release latches first
  ReleaseAllLatches(transaction, false);
  FindLeafPageForWrite(key, WriteType::DELETE, transaction);
  auto ans = LeafRemove(key, transaction);
  ReleaseAllLatches(transaction, ans);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }

  tree_latch_.RLock();
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  node->RLatch();
  tree_latch_.RUnlock();

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto first_page_id = internal->ValueAt(0);
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(first_page_id)->GetData());
    // acquire child's latch first
    node->RLatch();
    // release parent latch
    internal->RUnlatch();
    // unpin parent page
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  auto ans = INDEXITERATOR_TYPE(buffer_pool_manager_, node->GetPageId(), 0);

  node->RUnlatch();
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return ans;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }

  auto *leaf = FindLeafPageForRead(key);
  auto ans = INDEXITERATOR_TYPE(buffer_pool_manager_, leaf->GetPageId(), leaf->LowerBound(key, comparator_));

  leaf->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return ans;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }

  tree_latch_.RLock();
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  node->RLatch();
  tree_latch_.RUnlock();

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto last_page_id = internal->ValueAt(internal->GetSize() - 1);
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(last_page_id)->GetData());
    // acquire child's latch first
    node->RLatch();
    // release parent latch
    internal->RUnlatch();
    // unpin parent page
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  auto ans = INDEXITERATOR_TYPE(buffer_pool_manager_, node->GetPageId(), node->GetSize());

  node->RUnlatch();
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return ans;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  tree_latch_.RLock();
  auto ans = root_page_id_;
  tree_latch_.RUnlock();
  return ans;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForRead(const KeyType &key, bool write_latch_leaf, Transaction *txn) -> LeafPage * {
  tree_latch_.RLock();
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  if (write_latch_leaf && node->IsLeafPage()) {
    node->WLatch();
    txn->AddIntoPageSet(reinterpret_cast<Page *>(node));
  } else {
    node->RLatch();
  }
  tree_latch_.RUnlock();

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto index = internal->UpperBound(key, comparator_) - 1;
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(index))->GetData());
    // acquire child's latch first
    if (write_latch_leaf && node->IsLeafPage()) {
      node->WLatch();
      txn->AddIntoPageSet(reinterpret_cast<Page *>(node));
    } else {
      node->RLatch();
    }
    // release parent latch
    internal->RUnlatch();
    // unpin parent page
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  return reinterpret_cast<LeafPage *>(node);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForWrite(const KeyType &key, WriteType write_type, Transaction *txn) -> LeafPage * {
  auto page_set = txn->GetPageSet();

  tree_latch_.WLock();
  page_set->push_back(nullptr);
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  node->WLatch();
  page_set->push_back(reinterpret_cast<Page *>(node));

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto index = internal->UpperBound(key, comparator_) - 1;
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(index))->GetData());

    // acquire child's latch first
    node->WLatch();
    // release parent latch if current node is safe
    if (node->IsSafe(write_type)) {
      // release all previous latches
      while (!page_set->empty()) {
        auto *page = reinterpret_cast<BPlusTreePage *>(page_set->front());
        if (page != nullptr) {
          page->WUnlatch();
          // unpin parent page
          buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        } else {
          tree_latch_.WUnlock();
        }
        page_set->pop_front();
      }
    }
    page_set->push_back(reinterpret_cast<Page *>(node));
  }

  return reinterpret_cast<LeafPage *>(node);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentPageId(const page_id_t &child_page_id, const page_id_t &parent_page_id) {
  auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  child->SetParentPageId(parent_page_id);
  buffer_pool_manager_->UnpinPage(child_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  auto *leaf = reinterpret_cast<LeafPage *>(txn->GetPageSet()->back());
  if (!leaf->InsertKV(key, value, comparator_)) {
    return false;
  }

  // need split
  if (leaf->NeedSplit()) {
    // new leaf
    page_id_t new_leaf_page_id;
    auto *right = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_page_id)->GetData());
    // same parent page in default
    right->Init(new_leaf_page_id, leaf->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(right);
    // insert the new entry to parent node
    InternalInsert(leaf, right->KeyAt(0), right->GetPageId());
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalInsert(BPlusTreePage *left, const KeyType &key, const page_id_t &value) {
  if (left->IsRootPage()) {
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    UpdateRootPageId(false);
    parent->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    // init left most pointer to leaf
    parent->SetValueAt(0, left->GetPageId());
    parent->SetKeyAt(1, key);
    parent->SetValueAt(1, value);
    parent->IncreaseSize(1);

    UpdateParentPageId(left->GetPageId(), root_page_id_);
    UpdateParentPageId(value, root_page_id_);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left->GetParentPageId())->GetData());
  if (parent->IsSafe(WriteType::INSERT)) {
    parent->InsertKV(key, value, comparator_);
  } else {
    // new internal
    page_id_t new_internal_page_id;
    auto *right = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_page_id)->GetData());
    right->Init(new_internal_page_id, parent->GetParentPageId(), internal_max_size_);

    parent->MoveHalfAndInsert(right, key, value, comparator_);
    // update child's parent id
    RefreshChildParentId(right);

    InternalInsert(parent, right->KeyAt(0), right->GetPageId());
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafRemove(const KeyType &key, Transaction *txn) -> bool {
  auto latched_pages = txn->GetPageSet();
  auto *leaf = reinterpret_cast<LeafPage *>(latched_pages->back());
  auto index = leaf->LowerBound(key, comparator_);

  // key not exists
  if (index >= leaf->GetSize() || comparator_(key, leaf->KeyAt(index)) != 0) {
    return false;
  }

  auto min_key = leaf->KeyAt(0);
  // move data to previous index
  for (int i = index + 1; i < leaf->GetSize(); ++i) {
    leaf->SetKV(i - 1, leaf->GetKV(i));
  }
  leaf->IncreaseSize(-1);

  if (leaf->NeedMerge()) {
    LeafMerge(leaf, min_key, txn);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafMerge(LeafPage *leaf, const KeyType &min_key, Transaction *txn) {
  if (leaf->IsRootPage()) {
    return;
  }

  bool status = false;
  LeafPage *left = nullptr;
  LeafPage *right = nullptr;
  auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf->GetParentPageId())->GetData());
  auto index = parent->UpperBound(min_key, comparator_) - 1;

  // get left child
  if (index - 1 >= 0) {
    left = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
  }

  // get right child
  if (index + 1 < parent->GetSize()) {
    right = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1))->GetData());
  }

  // try borrow from left child
  if (left != nullptr) {
    left->WLatch();
    status = BorrowLeftLeaf(leaf, left, parent, index, txn);
    left->WUnlatch();
  }

  // try borrow from right child
  if (!status && right != nullptr) {
    right->WLatch();
    status = BorrowRightLeaf(leaf, right, parent, index, txn);
    right->WUnlatch();
  }

  // try merge left child
  if (!status && left != nullptr) {
    left->WLatch();
    status = LeafMergeRightToLeft(left, leaf, parent, index, txn);
    left->WUnlatch();
  }

  // try merge right child
  if (!status && right != nullptr) {
    right->WLatch();
    status = LeafMergeRightToLeft(leaf, right, parent, index + 1, txn);
    right->WUnlatch();
  }

  // unpin pages
  if (left != nullptr) {
    buffer_pool_manager_->UnpinPage(left->GetPageId(), status);
  }
  if (right != nullptr) {
    buffer_pool_manager_->UnpinPage(right->GetPageId(), status);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), status);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowLeftLeaf(LeafPage *leaf, LeafPage *left, InternalPage *parent, int leaf_index,
                                    Transaction *txn) -> bool {
  if (left->GetSize() <= left->GetMinSize()) {
    return false;
  }

  //  auto latched_pages = txn->GetPageSet();
  //  while (!latched_pages->empty()) {
  //    auto *page = reinterpret_cast<BPlusTreePage *>(latched_pages->front());
  //    if (page->GetPageId() == leaf->GetPageId()) {
  //      break;
  //    }
  //
  //    latched_pages->pop_front();
  //    page->WUnlatch();
  //    if (page->IsRootPage()) {
  //      // unlock tree latch if root node is safe
  //      tree_latch_.WUnlock();
  //    }
  //    // unpin parent page
  //    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //  }

  // move right most element from left sibling to leaf page
  for (int i = leaf->GetSize(); i > 0; --i) {
    leaf->SetKV(i, leaf->GetKV(i - 1));
  }
  leaf->SetKV(0, left->GetKV(left->GetSize() - 1));

  // update size
  leaf->IncreaseSize(1);
  left->IncreaseSize(-1);

  // update parent key
  parent->SetKeyAt(leaf_index, leaf->KeyAt(0));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowRightLeaf(LeafPage *leaf, LeafPage *right, InternalPage *parent, int leaf_index,
                                     Transaction *txn) -> bool {
  if (right->GetSize() <= right->GetMinSize()) {
    return false;
  }

  //  auto latched_pages = txn->GetPageSet();
  //  while (!latched_pages->empty()) {
  //    auto *page = reinterpret_cast<BPlusTreePage *>(latched_pages->front());
  //    if (page->GetPageId() == leaf->GetPageId()) {
  //      break;
  //    }
  //
  //    latched_pages->pop_front();
  //    page->WUnlatch();
  //    if (page->IsRootPage()) {
  //      // unlock tree latch if root node is safe
  //      tree_latch_.WUnlock();
  //    }
  //    // unpin parent page
  //    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //  }

  // move right most element from left sibling to leaf page
  leaf->SetKV(leaf->GetSize(), right->GetKV(0));
  for (int i = 1; i < right->GetSize(); ++i) {
    right->SetKV(i - 1, right->GetKV(i));
  }

  // update size
  leaf->IncreaseSize(1);
  right->IncreaseSize(-1);

  // update parent key
  parent->SetKeyAt(leaf_index + 1, right->KeyAt(0));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafMergeRightToLeft(LeafPage *left, LeafPage *right, InternalPage *parent, int right_index,
                                          Transaction *txn) -> bool {
  if (left == nullptr || right == nullptr) {
    return false;
  }
  assert(left->GetSize() + right->GetSize() < leaf_max_size_);

  // move data to left sibling
  for (int i = 0; i < right->GetSize(); ++i) {
    left->SetKV(left->GetSize() + i, right->GetKV(i));
  }

  // update size
  left->IncreaseSize(right->GetSize());

  // update next page id
  left->SetNextPageId(right->GetNextPageId());

  // delete right page
  txn->AddIntoDeletedPageSet(right->GetPageId());

  // save the min key before delete
  assert(parent->GetSize() > 1);
  auto min_key = parent->KeyAt(1);

  // update parent, delete right's correspond entry in parent
  for (int i = right_index + 1; i < parent->GetSize(); ++i) {
    parent->SetKV(i - 1, parent->GetKV(i));
  }
  parent->IncreaseSize(-1);

  // parent merge
  if (parent->NeedMerge()) {
    InternalMerge(parent, min_key, txn);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalMerge(InternalPage *internal, const KeyType &min_key, Transaction *txn) {
  if (internal->IsRootPage()) {
    // if root has only one child, make the child as root
    if (internal->GetSize() == 1) {
      // tree_page is root page, table_latch must be wlocked already, so can not lock tree_latch here
      // set parent id INVALID_PAGE_ID, a.k.a current node is root
      UpdateParentPageId(internal->ValueAt(0), INVALID_PAGE_ID);
      // update root_page_id_ field
      root_page_id_ = internal->ValueAt(0);
      UpdateRootPageId(false);

      // delete former root page
      txn->AddIntoDeletedPageSet(internal->GetPageId());
    }
    return;
  }

  bool status = false;
  InternalPage *left = nullptr;
  InternalPage *right = nullptr;
  auto *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal->GetParentPageId())->GetData());
  auto index = parent->UpperBound(min_key, comparator_) - 1;

  // get left child
  if (index - 1 >= 0) {
    left = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
  }

  // get right child
  if (index + 1 < parent->GetSize()) {
    right = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1))->GetData());
  }

  // try borrow from left child
  if (left != nullptr) {
    left->WLatch();
    status = BorrowLeftInternal(internal, left, parent, index, txn);
    left->WUnlatch();
  }

  // try borrow from right child
  if (!status && right != nullptr) {
    right->WLatch();
    status = BorrowRightInternal(internal, right, parent, index, txn);
    right->WUnlatch();
  }

  // try merge left child
  if (!status && left != nullptr) {
    left->WLatch();
    status = InternalMergeRightToLeft(left, internal, parent, index, txn);
    left->WUnlatch();
  }

  // try merge right child
  if (!status && right != nullptr) {
    right->WLatch();
    status = InternalMergeRightToLeft(internal, right, parent, index + 1, txn);
    right->WUnlatch();
  }

  // unpin pages
  if (left != nullptr) {
    buffer_pool_manager_->UnpinPage(left->GetPageId(), status);
  }
  if (right != nullptr) {
    buffer_pool_manager_->UnpinPage(right->GetPageId(), status);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), status);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowLeftInternal(InternalPage *internal, InternalPage *left, InternalPage *parent,
                                        int internal_index, Transaction *txn) -> bool {
  if (left->GetSize() <= left->GetMinSize()) {
    return false;
  }

  //  auto latched_pages = txn->GetPageSet();
  //  while (!latched_pages->empty()) {
  //    auto *page = reinterpret_cast<BPlusTreePage *>(latched_pages->front());
  //    if (page->GetPageId() == internal->GetPageId()) {
  //      break;
  //    }
  //
  //    latched_pages->pop_front();
  //    page->WUnlatch();
  //    if (page->IsRootPage()) {
  //      // unlock tree latch if root node is safe
  //      tree_latch_.WUnlock();
  //    }
  //    // unpin parent page
  //    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //  }

  // move internal's element to next location
  for (int i = internal->GetSize(); i > 0; --i) {
    internal->SetKV(i, internal->GetKV(i - 1));
  }

  // move parent key to current page's location 1
  internal->SetKeyAt(1, parent->KeyAt(internal_index));

  // update parent key
  parent->SetKeyAt(internal_index, left->KeyAt(left->GetSize() - 1));

  // move left sibling's right most value to internal's internal_index 0
  internal->SetValueAt(0, left->ValueAt(left->GetSize() - 1));
  // update child node's parent_id field
  UpdateParentPageId(internal->ValueAt(0), internal->GetPageId());

  // update size
  internal->IncreaseSize(1);
  left->IncreaseSize(-1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowRightInternal(InternalPage *internal, InternalPage *right, InternalPage *parent,
                                         int internal_index, Transaction *txn) -> bool {
  if (right->GetSize() <= right->GetMinSize()) {
    return false;
  }

  //  auto latched_pages = txn->GetPageSet();
  //  while (!latched_pages->empty()) {
  //    auto *page = reinterpret_cast<BPlusTreePage *>(latched_pages->front());
  //    if (page->GetPageId() == internal->GetPageId()) {
  //      break;
  //    }
  //
  //    latched_pages->pop_front();
  //    page->WUnlatch();
  //    if (page->IsRootPage()) {
  //      // unlock tree latch if root node is safe
  //      tree_latch_.WUnlock();
  //    }
  //    // unpin parent page
  //    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //  }

  // move parent key to internal's tail
  internal->SetKeyAt(internal->GetSize(), parent->KeyAt(internal_index + 1));

  // move right sibling's first value (internal_index 0) to internal's tail
  internal->SetValueAt(internal->GetSize(), right->ValueAt(0));
  // update child node's parent_id field
  UpdateParentPageId(internal->ValueAt(internal->GetSize()), internal->GetPageId());

  // update parent key
  parent->SetKeyAt(internal_index + 1, right->KeyAt(1));

  // move right sibling's data to previous position
  for (int i = 1; i < right->GetSize(); ++i) {
    right->SetKV(i - 1, right->GetKV(i));
  }

  // update size
  internal->IncreaseSize(1);
  right->IncreaseSize(-1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalMergeRightToLeft(InternalPage *left, InternalPage *right, InternalPage *parent,
                                              int right_index, Transaction *txn) -> bool {
  if (left == nullptr || right == nullptr) {
    return false;
  }
  assert(left->GetSize() + right->GetSize() <= internal_max_size_);

  // move data to left sibling
  for (int i = 0; i < right->GetSize(); ++i) {
    left->SetKV(left->GetSize() + i, right->GetKV(i));
    // update child node's parent_id field
    UpdateParentPageId(right->ValueAt(i), left->GetPageId());
  }
  // right[0]'s key is meaningless, need parent's key
  left->SetKeyAt(left->GetSize(), parent->KeyAt(right_index));

  // update size
  left->IncreaseSize(right->GetSize());

  // delete right page
  txn->AddIntoDeletedPageSet(right->GetPageId());

  // save the min key before delete
  assert(parent->GetSize() > 1);
  auto min_key = parent->KeyAt(1);

  // update parent
  for (int i = right_index + 1; i < parent->GetSize(); ++i) {
    parent->SetKV(i - 1, parent->GetKV(i));
  }
  parent->IncreaseSize(-1);

  // parent merge
  if (parent->NeedMerge()) {
    InternalMerge(parent, min_key, txn);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLatches(Transaction *txn, bool is_dirty) {
  auto page_set = txn->GetPageSet();
  while (!page_set->empty()) {
    auto *page = reinterpret_cast<BPlusTreePage *>(page_set->front());
    if (page != nullptr) {
      page->WUnlatch();
      // unpin parent page
      buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
    } else {
      tree_latch_.WUnlock();
    }
    page_set->pop_front();
  }

  auto deleted_pages = txn->GetDeletedPageSet();
  for (auto page_id : *deleted_pages) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  deleted_pages->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RefreshChildParentId(InternalPage *internal) {
  for (int i = 0; i < internal->GetSize(); ++i) {
    auto child_page_id = internal->ValueAt(i);
    auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id));
    child->SetParentPageId(internal->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
