//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  iterator_ = tree_->GetBeginIterator();
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == tree_->GetEndIterator()) {
    return false;
  }

  *rid = (*iterator_).second;

  auto txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(tree_->GetMetadata()->GetTableName());
  table->table_->GetTuple(*rid, tuple, txn);

  ++iterator_;
  return true;
}

}  // namespace bustub
