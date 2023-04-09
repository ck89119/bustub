/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_));
  auto ans = index_ == leaf->GetSize() && leaf->GetNextPageId() == INVALID_PAGE_ID;
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_));
  auto &&ans = leaf->GetKV(index_);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_));
  if (++index_ == leaf->GetSize() && leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_ = leaf->GetNextPageId();
    index_ = 0;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
