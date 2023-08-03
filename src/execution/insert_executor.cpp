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

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto table_oid = plan_->TableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(table_oid);
  auto indexes = catalog->GetTableIndexes(table->name_);

  if (NeedLock()) {
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("Insert get table IX lock failed");
    }
  }

  int inserted_cnt = 0;
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    if (!table->table_->InsertTuple(child_tuple, rid, txn)) {
      continue;
    }

    if (NeedLock()) {
      if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_oid, *rid)) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("Insert get row X lock failed");
      }
    }

    ++inserted_cnt;
    for (auto index_info : indexes) {
      auto index = index_info->index_.get();
      const Tuple index_key = child_tuple.KeyFromTuple(table->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
      index->InsertEntry(index_key, *rid, txn);
    }
  }

  *tuple = Tuple{{Value(TypeId::INTEGER, inserted_cnt)}, &GetOutputSchema()};
  executed_ = true;
  return true;
}

auto InsertExecutor::NeedLock() -> bool {
  auto isolation_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  return isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED;
}

}  // namespace bustub
