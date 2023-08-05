//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), on_table_(true) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid), on_table_(false) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};

    bool on_table_;
  };

  class LockRequestQueue {
   public:
    void Insert(const std::shared_ptr<LockRequest> &request, bool insert_head) {
      if (insert_head) {
        auto it = std::find_if(request_queue_.begin(), request_queue_.end(),
                               [](const std::shared_ptr<LockRequest> &req) -> bool { return !req->granted_; });
        request_queue_.insert(it, request);
      } else {
        request_queue_.emplace_back(request);
      }
    }

    auto IsGranted(const std::shared_ptr<LockRequest> &request, Transaction *txn, bool &aborted) -> bool {
      txn->LockTxn();

      if (txn->GetState() == TransactionState::ABORTED) {
        if (upgrading_ == request->txn_id_) {
          upgrading_ = INVALID_TXN_ID;
        }

        // if txn is aborted already, remove request in queue
        auto it = std::find(request_queue_.begin(), request_queue_.end(), request);
        assert(it != request_queue_.end());
        request_queue_.erase(it);

        aborted = true;
        txn->UnlockTxn();
        return true;
      }

      auto first_ungranted =
          std::find_if(request_queue_.begin(), request_queue_.end(),
                       [](const std::shared_ptr<LockRequest> &req) -> bool { return !req->granted_; });
      if (request != *first_ungranted) {
        txn->UnlockTxn();
        return false;
      }

      auto lock_mode = request->lock_mode_;
      auto compatible_modes = compatible_map_[lock_mode];
      for (auto it = request_queue_.begin(); it != first_ungranted; ++it) {
        // not compatible
        if (std::find(compatible_modes.begin(), compatible_modes.end(), (*it)->lock_mode_) == compatible_modes.end()) {
          txn->UnlockTxn();
          return false;
        }
      }

      request->granted_ = true;
      if (upgrading_ == request->txn_id_) {
        upgrading_ = INVALID_TXN_ID;
      }
      // update responding lock set in txn
      if (request->on_table_) {
        GetTableLockSetByMode(txn, static_cast<int>(lock_mode))->insert(request->oid_);
      } else {
        (*GetRowLockSetByMode(txn, static_cast<int>(lock_mode)))[request->oid_].insert(request->rid_);
      }

      txn->UnlockTxn();
      return true;
    }

    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;

    std::unordered_map<LockMode, std::vector<LockMode>> compatible_map_ = {
        {LockMode::INTENTION_SHARED,
         {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED,
          LockMode::SHARED_INTENTION_EXCLUSIVE}},
        {LockMode::INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE}},
        {LockMode::SHARED, {LockMode::INTENTION_SHARED, LockMode::SHARED}},
        {LockMode::SHARED_INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED}}};
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);

    // IS -> [S, X, IX, SIX]
    // S -> [X, SIX]
    // IX -> [X, SIX]
    // SIX -> [X]
    upgrade_map_ = {
        {LockMode::INTENTION_SHARED,
         {LockMode::SHARED, LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}},
        {LockMode::SHARED, {LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}},
        {LockMode::INTENTION_EXCLUSIVE, {LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}},
        {LockMode::SHARED_INTENTION_EXCLUSIVE, {LockMode::EXCLUSIVE}}};
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  void SetCycleDetection(bool flag) { enable_cycle_detection_ = flag; }

 private:
  auto Upgradable(LockMode from, LockMode to) -> bool;

  auto LockPreCheck(Transaction *txn, LockMode mode, bool on_table, const table_oid_t &table_id, AbortReason &reason)
      -> bool;

  auto UnlockPreCheck(Transaction *txn, bool on_table, const table_oid_t &table_id, const RID &rid, bool from_upgrade,
                      AbortReason &reason) -> bool;

  auto GetQueue(const table_oid_t &oid) -> std::shared_ptr<LockRequestQueue>;

  auto UnlockTableHelper(Transaction *txn, const table_oid_t &oid, bool from_upgrade) -> bool;

  auto GetQueue(const RID &rid) -> std::shared_ptr<LockRequestQueue>;

  auto UnlockRowHelper(Transaction *txn, const table_oid_t &oid, const RID &rid, bool from_upgrade) -> bool;

  auto Dfs(txn_id_t u, std::map<txn_id_t, int> &colors, std::vector<txn_id_t> &path, txn_id_t &max_txn_id) -> bool;

  void BuildWaitsForGraph();

  void NotifyAll();

  static auto GetRowLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid) -> int {
    // enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
    if (txn->IsRowSharedLocked(oid, rid)) {
      return static_cast<int>(LockMode::SHARED);  // 0
    }

    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return static_cast<int>(LockMode::EXCLUSIVE);  // 1
    }

    return -1;
  }

  static auto GetRowLockSetByMode(Transaction *txn, int mode)
      -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
    if (static_cast<LockMode>(mode) == LockMode::SHARED) {
      return txn->GetSharedRowLockSet();  // 0
    }

    if (static_cast<LockMode>(mode) == LockMode::EXCLUSIVE) {
      return txn->GetExclusiveRowLockSet();  // 1
    }

    return nullptr;
  }

  static auto GetTableLockMode(Transaction *txn, const table_oid_t &oid) -> int {
    // enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

    if (txn->IsTableSharedLocked(oid)) {
      return static_cast<int>(LockMode::SHARED);  // 0
    }

    if (txn->IsTableExclusiveLocked(oid)) {
      return static_cast<int>(LockMode::EXCLUSIVE);  // 1
    }

    if (txn->IsTableIntentionSharedLocked(oid)) {
      return static_cast<int>(LockMode::INTENTION_SHARED);  // 2
    }

    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      return static_cast<int>(LockMode::INTENTION_EXCLUSIVE);  // 3
    }

    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return static_cast<int>(LockMode::SHARED_INTENTION_EXCLUSIVE);  // 4
    }

    return -1;
  }

  static auto GetTableLockSetByMode(Transaction *txn, int mode) -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    if (static_cast<LockMode>(mode) == LockMode::SHARED) {  // 0
      return txn->GetSharedTableLockSet();
    }

    if (static_cast<LockMode>(mode) == LockMode::EXCLUSIVE) {  // 1
      return txn->GetExclusiveTableLockSet();
    }

    if (static_cast<LockMode>(mode) == LockMode::INTENTION_SHARED) {  // 2
      return txn->GetIntentionSharedTableLockSet();
    }

    if (static_cast<LockMode>(mode) == LockMode::INTENTION_EXCLUSIVE) {  // 3
      return txn->GetIntentionExclusiveTableLockSet();
    }

    if (static_cast<LockMode>(mode) == LockMode::SHARED_INTENTION_EXCLUSIVE) {  // 4
      return txn->GetSharedIntentionExclusiveTableLockSet();
    }

    return nullptr;
  }

  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  std::unordered_map<LockMode, std::vector<LockMode>> upgrade_map_;
};

}  // namespace bustub
