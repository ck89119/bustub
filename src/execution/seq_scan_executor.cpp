//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto table_oid = plan_->GetTableOid();

  if (NeedLock()) {
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("Get table IS lock failed");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto isolation_level = txn->GetIsolationLevel();
  auto table_oid = plan_->GetTableOid();

  if (iterator_ == exec_ctx_->GetCatalog()->GetTable(table_oid)->table_->End()) {
    return false;
  }

  bool row_locked = false;
  *rid = iterator_->GetRid();
  if (NeedLock()) {
    if (!lock_manager->LockRow(txn, LockManager::LockMode::SHARED, table_oid, *rid)) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("Get row S lock failed");
    }
    row_locked = true;
  }
  *tuple = *iterator_++;
  if (row_locked && isolation_level == IsolationLevel::READ_COMMITTED) {
    lock_manager->UnlockRow(txn, table_oid, *rid);
  }
  return true;
}

auto SeqScanExecutor::NeedLock() -> bool {
  auto isolation_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  return isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED;
}

}  // namespace bustub
