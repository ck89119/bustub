//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), executed_(false) {}

void InsertExecutor::Init() {
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->table_oid_);
  auto indexes = catalog->GetTableIndexes(table->name_);

  int inserted_cnt = 0;
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    if (!table->table_->InsertTuple(child_tuple, rid, txn)) {
      continue;
    }

    ++inserted_cnt;
    for (auto index_info: indexes) {
      auto index = index_info->index_.get();
      const Tuple index_key = child_tuple.KeyFromTuple(table->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->InsertEntry(index_key, *rid, txn);
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, inserted_cnt)}, &GetOutputSchema()};
  executed_ = true;
  return true;
}

}  // namespace bustub
