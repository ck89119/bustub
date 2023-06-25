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
    : buffer_pool_manager_(buffer_pool_manager), index_(index) {
  leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id));
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return index_ >= leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetKV(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (++index_ >= leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page_id = leaf_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

    index_ = 0;
    leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page_id));
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
