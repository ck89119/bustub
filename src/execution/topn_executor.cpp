#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto cmp = [&](const Tuple &a, const Tuple &b) -> bool {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple tuple{};
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    topn_.emplace_back(pq.top());
    pq.pop();
  }
  cursor_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (topn_.empty()) {
    return false;
  }

  *tuple = topn_.back();
  *rid = tuple->GetRid();
  topn_.pop_back();
  return true;
}

}  // namespace bustub
