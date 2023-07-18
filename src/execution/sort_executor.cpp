#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple tuple{};
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto &[type, expr] : plan_->GetOrderBy()) {
      auto value_a = expr->Evaluate(&a, plan_->OutputSchema());
      auto value_b = expr->Evaluate(&b, plan_->OutputSchema());

      if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
        continue;
      }

      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        return value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
      }

      if (type == OrderByType::DESC) {
        return value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue;
      }
    }
    return true;
  });

  cursor_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[cursor_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
