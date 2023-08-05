//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::Upgradable(LockMode from, LockMode to) -> bool {
  auto vec = upgrade_map_[from];
  return std::find(vec.begin(), vec.end(), to) != vec.end();
}

auto LockManager::LockPreCheck(Transaction *txn, LockMode mode, bool on_table, const table_oid_t &table_id,
                               AbortReason &reason) -> bool {
  auto state = txn->GetState();
  auto isolation_level = txn->GetIsolationLevel();

  // Row locking not support Intention locks
  if (!on_table && mode != LockMode::SHARED && mode != LockMode::EXCLUSIVE) {
    reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
    return false;
  }

  // isolation level
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (mode == LockMode::SHARED || mode == LockMode::INTENTION_SHARED ||
        mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }

    if (state == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (state == TransactionState::SHRINKING && (mode == LockMode::EXCLUSIVE || mode == LockMode::INTENTION_EXCLUSIVE ||
                                                 mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (state == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  // multiple-level locking
  if (!on_table) {
    auto code = GetTableLockMode(txn, table_id);
    if (mode == LockMode::EXCLUSIVE) {
      auto x_code = static_cast<int>(LockMode::EXCLUSIVE);
      auto ix_code = static_cast<int>(LockMode::INTENTION_EXCLUSIVE);
      auto six_code = static_cast<int>(LockMode::SHARED_INTENTION_EXCLUSIVE);
      // need X, IX, or SIX lock on table
      if (code != x_code && code != ix_code && code != six_code) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else {
      // any lock on table suffices
      if (code == -1) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
  }

  return true;
}

auto LockManager::UnlockPreCheck(Transaction *txn, bool on_table, const table_oid_t &table_id, const RID &rid,
                                 bool from_upgrade, AbortReason &reason) -> bool {
  int code = on_table ? GetTableLockMode(txn, table_id) : GetRowLockMode(txn, table_id, rid);
  if (code == -1) {
    reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
    return false;
  }

  // make sure all row unlocked before unlock table, ignore upgrade case
  if (on_table && !from_upgrade) {
    auto shared_set = (*txn->GetSharedRowLockSet())[table_id];
    auto exclusive_set = (*txn->GetExclusiveRowLockSet())[table_id];
    if (!shared_set.empty() || !exclusive_set.empty()) {
      reason = AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS;
      return false;
    }
  }

  return true;
}

auto LockManager::GetQueue(const table_oid_t &oid) -> std::shared_ptr<LockRequestQueue> {
  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  return table_lock_map_[oid];
};

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto queue = GetQueue(oid);
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();

  auto txn_id = txn->GetTransactionId();
  AbortReason reason;
  if (!LockPreCheck(txn, lock_mode, true, oid, reason)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, reason);
  }

  // check txn hold this table lock or not
  auto code = GetTableLockMode(txn, oid);
  bool held_lock_already = code != -1;

  // txn have held table(oid) lock with some mode, need upgrade the lock
  // upgrade = remove old lock + add new lock
  if (held_lock_already) {
    auto held_lock_mode = static_cast<LockMode>(code);
    // same mode with held lock, return true immediately
    if (held_lock_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }

    // if cannot upgrade, throw exception
    if (!Upgradable(held_lock_mode, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // other txn is upgrading
    if (queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    queue->upgrading_ = txn_id;

    // unlock table first
    // hold queue latch & txn lock
    UnlockTableHelper(txn, oid, true);
  }

  // unlock txn asap for performance consideration
  txn->UnlockTxn();

  // insert tail of queue
  auto request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  queue->Insert(request, held_lock_already);

  // wait until grant lock to this txn
  bool aborted = false;
  while (!queue->IsGranted(request, txn, aborted)) {
    queue->cv_.wait(lock);
  }
  queue->cv_.notify_all();

  return !aborted;
}

auto LockManager::UnlockTableHelper(Transaction *txn, const table_oid_t &oid, bool from_upgrade) -> bool {
  auto queue = GetQueue(oid);
  std::unique_lock<std::mutex> lock;
  if (!from_upgrade) {
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }

  AbortReason reason;
  if (!UnlockPreCheck(txn, true, oid, RID(), from_upgrade, reason)) {
    txn->SetState(TransactionState::ABORTED);
    if (!from_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  // update txn
  // code(S) = 0, code(X) = 1
  auto isolation_level = txn->GetIsolationLevel();
  auto code = GetTableLockMode(txn, oid);
  auto need_update_state = [&]() -> bool {
    return (isolation_level == IsolationLevel::REPEATABLE_READ && code <= 1) ||
           (isolation_level == IsolationLevel::READ_COMMITTED && code == 1) ||
           (isolation_level == IsolationLevel::READ_UNCOMMITTED && code == 1);
  };
  if (!from_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED &&
      need_update_state()) {
    txn->SetState(TransactionState::SHRINKING);
  }
  GetTableLockSetByMode(txn, code)->erase(oid);

  // update queue
  auto &request_queue = queue->request_queue_;
  auto it = std::find_if(
      request_queue.begin(), request_queue.end(),
      [&](const std::shared_ptr<LockRequest> &req) -> bool { return req->txn_id_ == txn->GetTransactionId(); });
  if (it != request_queue.end()) {
    request_queue.erase(it);
  }

  if (!from_upgrade) {
    // if upgrade, dont awaken other candidates
    queue->cv_.notify_all();
    txn->UnlockTxn();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  return UnlockTableHelper(txn, oid, false);
}

auto LockManager::GetQueue(const RID &rid) -> std::shared_ptr<LockRequestQueue> {
  std::unique_lock<std::mutex> lock(row_lock_map_latch_);
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  return row_lock_map_[rid];
};

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  auto queue = GetQueue(rid);
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();

  auto txn_id = txn->GetTransactionId();
  AbortReason reason;
  if (!LockPreCheck(txn, lock_mode, false, oid, reason)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, reason);
  }

  // check txn hold this row lock or not
  auto code = GetRowLockMode(txn, oid, rid);
  bool held_lock_already = code != -1;

  // txn have held row(oid, rid) lock with some mode, need upgrade the lock
  // upgrade = remove old lock + add new lock
  if (held_lock_already) {
    auto held_lock_mode = static_cast<LockMode>(code);
    // same mode with held lock, return true immediately
    if (held_lock_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }

    // if cannot upgrade, throw exception
    if (!Upgradable(held_lock_mode, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // other txn is upgrading
    if (queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    queue->upgrading_ = txn_id;

    // unlock row lock first
    // hold queue latch & txn lock
    UnlockRowHelper(txn, oid, rid, true);
  }

  // unlock txn for performance consideration
  txn->UnlockTxn();

  // insert tail of queue
  auto request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  queue->Insert(request, held_lock_already);

  // wait until grant lock to this txn
  bool aborted = false;
  while (!queue->IsGranted(request, txn, aborted)) {
    queue->cv_.wait(lock);
  }
  queue->cv_.notify_all();

  return !aborted;
}

auto LockManager::UnlockRowHelper(Transaction *txn, const table_oid_t &oid, const RID &rid, bool from_upgrade) -> bool {
  auto queue = GetQueue(rid);
  std::unique_lock<std::mutex> lock;
  if (!from_upgrade) {
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }

  AbortReason reason;
  if (!UnlockPreCheck(txn, false, oid, rid, from_upgrade, reason)) {
    txn->SetState(TransactionState::ABORTED);
    if (!from_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  // update txn
  // code(S) = 0, code(X) = 1
  auto code = GetRowLockMode(txn, oid, rid);
  auto isolation_level = txn->GetIsolationLevel();
  auto need_update_state = [&]() -> bool {
    return (isolation_level == IsolationLevel::REPEATABLE_READ && code <= 1) ||
           (isolation_level == IsolationLevel::READ_COMMITTED && code == 1) ||
           (isolation_level == IsolationLevel::READ_UNCOMMITTED && code == 1);
  };
  if (!from_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED &&
      need_update_state()) {
    txn->SetState(TransactionState::SHRINKING);
  }
  (*GetRowLockSetByMode(txn, code))[oid].erase(rid);

  // update queue
  auto &request_queue = queue->request_queue_;
  auto it = std::find_if(
      request_queue.begin(), request_queue.end(),
      [&](const std::shared_ptr<LockRequest> &req) -> bool { return req->txn_id_ == txn->GetTransactionId(); });
  if (it != request_queue.end()) {
    request_queue.erase(it);
  }

  if (!from_upgrade) {
    // if upgrade, dont awaken other candidates
    queue->cv_.notify_all();
    txn->UnlockTxn();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  return UnlockRowHelper(txn, oid, rid, false);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("AddEdge t1 = %d, t2 = %d", t1, t2);
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("RemoveEdge t1 = %d, t2 = %d", t1, t2);
  auto &v = waits_for_[t1];
  auto it = std::find(v.begin(), v.end(), t2);
  if (it != v.end()) {
    v.erase(it);
  }
}

auto LockManager::Dfs(txn_id_t u, std::map<txn_id_t, int> &colors, std::vector<txn_id_t> &path, txn_id_t &max_txn_id)
    -> bool {
  path.push_back(u);
  if (colors[u] == 1) {
    auto it = std::find(path.begin(), path.end(), path.back());
    while (it != path.end()) {
      max_txn_id = std::max(max_txn_id, *it++);
    }
    return true;
  }
  colors[u] = 1;

  for (auto v : waits_for_[u]) {
    if (colors[v] != 2 && Dfs(v, colors, path, max_txn_id)) {
      // find first circle, return immediately
      return true;
    }
  }

  colors[u] = 2;
  path.pop_back();
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::map<txn_id_t, int> colors;
  for (auto [k, v] : waits_for_) {
    colors.emplace(k, 0);
  }

  *txn_id = INVALID_TXN_ID;
  for (auto [k, v] : colors) {
    if (v == 2) {
      continue;
    }

    std::vector<txn_id_t> path;
    if (Dfs(k, colors, path, *txn_id)) {
      // find first circle, return directly
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[u, vs] : waits_for_) {
    for (auto v : vs) {
      edges.emplace_back(u, v);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
      std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);

      BuildWaitsForGraph();
      bool has_cycle = false;
      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(&txn_id)) {
        LOG_INFO("has cycle, txn_id = %d", txn_id);
        has_cycle = true;
        // update waits_for
        waits_for_.erase(txn_id);
        for (auto &[k, v] : waits_for_) {
          RemoveEdge(k, txn_id);
        }
        // update txn
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
      }

      if (has_cycle) {
        NotifyAll();
      }
    }
  }
}

void LockManager::BuildWaitsForGraph() {
  LOG_INFO("BuildWaitsForGraph");
  waits_for_.clear();

  for (auto &[_, queue] : table_lock_map_) {
    std::unique_lock<std::mutex> lock(queue->latch_);
    std::vector<txn_id_t> granted;
    for (const auto &request : queue->request_queue_) {
      if (request->granted_) {
        granted.emplace_back(request->txn_id_);
      } else {
        for (auto u : granted) {
          AddEdge(u, request->txn_id_);
        }
      }
    }
  }

  for (auto &[_, queue] : row_lock_map_) {
    std::unique_lock<std::mutex> lock(queue->latch_);
    std::vector<txn_id_t> granted;
    for (const auto &request : queue->request_queue_) {
      if (request->granted_) {
        granted.emplace_back(request->txn_id_);
      } else {
        for (auto u : granted) {
          AddEdge(u, request->txn_id_);
        }
      }
    }
  }

  for (auto &[_, v] : waits_for_) {
    std::sort(v.begin(), v.end());
  }
}

void LockManager::NotifyAll() {
  for (auto &[_, queue] : table_lock_map_) {
    queue->cv_.notify_all();
  }

  for (auto &[_, queue] : row_lock_map_) {
    queue->cv_.notify_all();
  }
}

}  // namespace bustub
