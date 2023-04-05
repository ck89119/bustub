#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
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
      internal_max_size_(internal_max_size) {}

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
  if (IsEmpty()) {
    return false;
  }

  LeafPage *leaf = FindLeafPage(key);
  auto index = leaf->LowerBound(key, comparator_);
  // key exists
  bool status = false;
  if (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) == 0) {
    result->push_back(leaf->ValueAt(index));
    status = true;
  }

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
  LeafPage *leaf = nullptr;
  if (IsEmpty()) {
    leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->SetNextPageId(INVALID_PAGE_ID);
    UpdateRootPageId(true);
  } else {
    leaf = FindLeafPage(key);
  }

  auto ans = LeafInsert(leaf, key, value);

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), ans);
  return ans;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  LeafPage *leaf = FindLeafPage(key);
  auto index = leaf->LowerBound(key, comparator_);
  // key not exists
  if (index >= leaf->GetSize() || comparator_(key, leaf->KeyAt(index)) != 0) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }

  // min key in this page
  auto min_key = leaf->KeyAt(0);

  // move data to previous index
  for (int i = index + 1; i < leaf->GetSize(); ++i) {
    leaf->SetKV(i - 1, leaf->GetKV(i));
  }
  leaf->IncreaseSize(-1);

  if (leaf->NeedMerge()) {
    LeafMerge(leaf, min_key);
  }

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) const -> LeafPage * {
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);

    auto index = internal->UpperBound(key, comparator_);
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(index - 1)));

    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  return reinterpret_cast<LeafPage *>(node);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetParent(BPlusTreePage *tree_page, bool create_if_not_exsit) -> InternalPage * {
  InternalPage *parent = nullptr;
  // tree page is root page
  if (tree_page->GetParentPageId() == INVALID_PAGE_ID) {
    // create a new root page as its parent
    if (create_if_not_exsit) {
      parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
      parent->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      UpdateRootPageId(false);
      // init left most pointer to leaf
      parent->SetValueAt(0, tree_page->GetPageId());
    }
  } else {
    parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(tree_page->GetParentPageId()));
  }
  return parent;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentPageId(const page_id_t &child_page_id, const page_id_t &parent_page_id) {
  auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id));
  child->SetParentPageId(parent_page_id);
  buffer_pool_manager_->UnpinPage(child_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafInsert(LeafPage *leaf, const KeyType &key, const ValueType &value) -> bool {
  auto index = leaf->LowerBound(key, comparator_);
  // key exists
  if (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) == 0) {
    return false;
  }

  // move to next location
  for (int i = leaf->GetSize(); i > index; --i) {
    leaf->SetKV(i, leaf->GetKV(i - 1));
  }
  leaf->SetKV(index, {key, value});
  leaf->IncreaseSize(1);

  // need split
  if (leaf->NeedSplit()) {
    LeafSplit(leaf);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafSplit(LeafPage *leaf) {
  // new leaf
  page_id_t new_leaf_page_id;
  auto *new_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_page_id));
  new_leaf->Init(new_leaf_page_id, leaf->GetParentPageId(), leaf_max_size_);

  // move data [middle, max_size) to new leaf
  auto max_size = leaf->GetMaxSize();
  int middle = max_size / 2;
  for (int i = middle; i < max_size; ++i) {
    new_leaf->SetKV(i - middle, leaf->GetKV(i));
  }

  // set size
  new_leaf->SetSize(max_size - middle);
  leaf->SetSize(middle);

  // set next page id
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_page_id);

  // get parent
  InternalPage *parent = GetParent(leaf, true);

  // update leaves' parent id
  leaf->SetParentPageId(parent->GetPageId());
  new_leaf->SetParentPageId(parent->GetPageId());

  // insert the new entry to parent node
  InternalInsert(parent, new_leaf->KeyAt(0), new_leaf_page_id);

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalInsert(InternalPage *internal, const KeyType &key, const page_id_t &value) {
  if (internal->NeedSplit()) {
    InternalSplit(internal, key, value);
    return;
  }

  internal->InsertKV(key, value, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalSplit(InternalPage *internal, const KeyType &key, const page_id_t &value) {
  // internal level part
  // new internal
  page_id_t new_internal_page_id;
  auto *new_internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_page_id));
  new_internal->Init(new_internal_page_id, internal->GetParentPageId(), internal_max_size_);

  // move data [middle, max_size) to new leaf
  auto max_size = internal->GetMaxSize();
  // sizeof(right) == sizeof(left) or sizeof(left) + 1
  int middle = max_size / 2;
  if (internal->UpperBound(key, comparator_) >= middle) {
    middle += 1;
  }

  for (int i = middle; i < max_size; ++i) {
    new_internal->SetKV(i - middle, internal->GetKV(i));
    // update child's parent id
    UpdateParentPageId(internal->ValueAt(i), new_internal_page_id);
  }

  // set size
  new_internal->SetSize(max_size - middle);
  internal->SetSize(middle);

  // insert KV into page
  InternalPage *page_to_insert = internal->UpperBound(key, comparator_) < internal->GetSize() ? internal : new_internal;
  page_to_insert->InsertKV(key, value, comparator_);
  // update child's parent id
  UpdateParentPageId(value, page_to_insert->GetPageId());

  // parent level part
  // insert the new entry to parent node
  InternalPage *parent = GetParent(internal, true);

  // update parent id
  internal->SetParentPageId(parent->GetPageId());
  new_internal->SetParentPageId(parent->GetPageId());

  InternalInsert(parent, new_internal->KeyAt(0), new_internal_page_id);

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_internal_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromSiblingLeaf(LeafPage *leaf, const KeyType &key) -> bool {
  InternalPage *parent = GetParent(leaf, false);
  // if leaf is root page, no siblings to borrow
  if (parent == nullptr) {
    return false;
  }

  auto index = parent->UpperBound(key, comparator_) - 1;
  // if have left sibling and left sibling's size > min_size, borrow one entry
  if (index - 1 >= 0) {
    bool ans = false;
    auto *left_sibling_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1)));
    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      // move right most element from left sibling to leaf page
      for (int i = leaf->GetSize(); i > 0; --i) {
        leaf->SetKV(i, leaf->GetKV(i - 1));
      }
      leaf->SetKV(0, left_sibling_page->GetKV(left_sibling_page->GetSize() - 1));
      // leaf node has no need to update parent id

      // update size
      leaf->IncreaseSize(1);
      left_sibling_page->IncreaseSize(-1);

      // update parent key
      parent->SetKeyAt(index, leaf->KeyAt(0));

      ans = true;
    }

    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ans;
  }

  // borrow from right sibling
  if (index + 1 < parent->GetSize()) {
    bool ans = false;
    auto *right_sibling_page =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1)));
    if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
      // move right most element from left sibling to leaf page
      leaf->SetKV(leaf->GetSize(), right_sibling_page->GetKV(0));
      for (int i = 1; i < right_sibling_page->GetSize(); ++i) {
        right_sibling_page->SetKV(i - 1, right_sibling_page->GetKV(i));
      }

      // update size
      leaf->IncreaseSize(1);
      right_sibling_page->IncreaseSize(-1);

      // update parent key
      parent->SetKeyAt(index + 1, right_sibling_page->KeyAt(0));

      ans = true;
    }

    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ans;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafMerge(LeafPage *leaf, const KeyType &key) {
  if (BorrowFromSiblingLeaf(leaf, key)) {
    return;
  }

  InternalPage *parent = GetParent(leaf, false);
  // if leaf is root page, no siblings to merge with
  if (parent == nullptr) {
    return;
  }

  auto min_key = parent->KeyAt(1);
  auto index = parent->UpperBound(key, comparator_) - 1;
  // if have left sibling
  if (index - 1 >= 0) {
    auto *left_sibling_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1)));
    assert(leaf->GetSize() + left_sibling_page->GetSize() < leaf_max_size_);

    // move data to left sibling
    for (int i = 0; i < leaf->GetSize(); ++i) {
      left_sibling_page->SetKV(left_sibling_page->GetSize() + i, leaf->GetKV(i));
    }

    // update size
    left_sibling_page->IncreaseSize(leaf->GetSize());

    // update next page id
    left_sibling_page->SetNextPageId(leaf->GetNextPageId());

    // delete leaf page
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    buffer_pool_manager_->DeletePage(leaf->GetPageId());
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);

    // update parent
    for (int i = index + 1; i < parent->GetSize(); ++i) {
      parent->SetKV(i - 1, parent->GetKV(i));
    }
    parent->IncreaseSize(-1);

    // parent merge
    if (parent->NeedMerge()) {
      InternalMerge(parent, min_key);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  // if have right sibling
  if (index + 1 < parent->GetSize()) {
    auto *right_sibling_page =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1)));
    assert(leaf->GetSize() + right_sibling_page->GetSize() < leaf_max_size_);

    // move right sibling's data to current leaf
    for (int i = 0; i < right_sibling_page->GetSize(); ++i) {
      leaf->SetKV(leaf->GetSize() + i, right_sibling_page->GetKV(i));
    }

    // update size
    leaf->IncreaseSize(right_sibling_page->GetSize());

    // update next page id
    leaf->SetNextPageId(right_sibling_page->GetNextPageId());

    // delete right sibling page
    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(right_sibling_page->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

    // update parent
    for (int i = index + 2; i < parent->GetSize(); ++i) {
      parent->SetKV(i - 1, parent->GetKV(i));
    }
    parent->IncreaseSize(-1);

    // parent merge
    if (parent->NeedMerge()) {
      InternalMerge(parent, min_key);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromSiblingInternal(InternalPage *internal, const KeyType &key) -> bool {
  InternalPage *parent = GetParent(internal, false);
  // if internal is root page, no siblings to borrow
  if (parent == nullptr) {
    return false;
  }

  auto index = parent->UpperBound(key, comparator_) - 1;
  // if have left sibling and left sibling's size > min_size, borrow one entry
  if (index - 1 >= 0) {
    auto *left_sibling_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1)));
    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      // move internal's data to next position (Value in index 0 is not set, Key in index 1 is not set)
      for (int i = internal->GetSize(); i > 0; --i) {
        internal->SetKV(i, internal->GetKV(i - 1));
      }

      // move parent key to internal's index 1
      internal->SetKeyAt(1, parent->KeyAt(index));

      // move left sibling's right most value to internal's index 0
      internal->SetValueAt(0, left_sibling_page->ValueAt(left_sibling_page->GetSize() - 1));
      // update child node's parent_id field
      UpdateParentPageId(internal->ValueAt(0), internal->GetPageId());

      // update parent key
      parent->SetKeyAt(index, left_sibling_page->KeyAt(left_sibling_page->GetSize() - 1));

      // update size
      internal->IncreaseSize(1);
      left_sibling_page->IncreaseSize(-1);
    }

    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return true;
  }

  // borrow from right sibling
  if (index + 1 < parent->GetSize()) {
    auto *right_sibling_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1)));
    if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
      // move parent key to internal's tail
      internal->SetKeyAt(internal->GetSize(), parent->KeyAt(index));

      // move right sibling's first value (index 0) to interal's tail
      internal->SetValueAt(internal->GetSize(), right_sibling_page->ValueAt(0));
      // update child node's parent_id field
      UpdateParentPageId(right_sibling_page->ValueAt(0), internal->GetPageId());

      // update parent key
      parent->SetKeyAt(index, right_sibling_page->KeyAt(1));

      // move right sibling's data to previous position
      for (int i = 1; i < right_sibling_page->GetSize(); ++i) {
        right_sibling_page->SetKV(i - 1, right_sibling_page->GetKV(i));
      }

      // update size
      internal->IncreaseSize(1);
      right_sibling_page->IncreaseSize(-1);
    }

    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return true;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalMerge(InternalPage *internal, const KeyType &key) {
  if (BorrowFromSiblingInternal(internal, key)) {
    return;
  }

  InternalPage *parent = GetParent(internal, false);
  // if internal is root, no siblings to merge with
  if (parent == nullptr) {
    // if root has only one child, make the child as root
    if (internal->GetSize() == 1) {
      // set parent id INVALID_PAGE_ID, a.k.a current node is root
      UpdateParentPageId(internal->ValueAt(0), INVALID_PAGE_ID);
      // update root_page_id_ field
      root_page_id_ = internal->ValueAt(0);
      UpdateRootPageId(false);

      // delete former root page
      buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
      buffer_pool_manager_->DeletePage(internal->GetPageId());
    }
    return;
  }

  auto min_key = parent->KeyAt(1);
  auto index = parent->UpperBound(key, comparator_) - 1;
  // if have left sibling
  if (index - 1 >= 0) {
    auto *left_sibling_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1)));
    assert(internal->GetSize() + left_sibling_page->GetSize() < internal_max_size_);

    // move internal's data to left sibling's tail
    for (int i = 0; i < internal->GetSize(); ++i) {
      left_sibling_page->SetKV(left_sibling_page->GetSize() + i, internal->GetKV(i));
      // update child node's parent_id field
      UpdateParentPageId(internal->ValueAt(i), left_sibling_page->GetPageId());
    }

    // move parent key
    left_sibling_page->SetKeyAt(left_sibling_page->GetSize(), parent->KeyAt(index));

    // move parent's data to previous position
    for (int i = index + 1; i < parent->GetSize(); ++i) {
      parent->SetKV(i - 1, parent->GetKV(i));
    }

    // update size
    left_sibling_page->IncreaseSize(internal->GetSize());
    parent->IncreaseSize(-1);

    // delete internal page
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
    buffer_pool_manager_->DeletePage(internal->GetPageId());
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);

    // parent merge
    if (parent->NeedMerge()) {
      InternalMerge(parent, min_key);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  // if have right sibling
  if (index + 1 < parent->GetSize()) {
    auto *right_sibling_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1)));
    assert(internal->GetSize() + right_sibling_page->GetSize() < internal_max_size_);

    // move right sibling's data to internal's tail
    for (int i = 0; i < right_sibling_page->GetSize(); ++i) {
      internal->SetKV(internal->GetSize() + i, right_sibling_page->GetKV(i));
      // update child node's parent_id field
      UpdateParentPageId(right_sibling_page->ValueAt(i), internal->GetPageId());
    }

    // move parent key
    internal->SetKeyAt(internal->GetSize(), parent->KeyAt(index + 1));

    // move parent's data to previous position
    for (int i = index + 2; i < parent->GetSize(); ++i) {
      parent->SetKV(i - 1, parent->GetKV(i));
    }

    // update size
    internal->IncreaseSize(right_sibling_page->GetSize());
    parent->IncreaseSize(-1);

    // delete right sibling page
    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(right_sibling_page->GetPageId());
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);

    // parent merge
    if (parent->NeedMerge()) {
      InternalMerge(parent, min_key);
    }

    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
