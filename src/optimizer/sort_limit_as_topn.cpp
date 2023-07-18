#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }
  const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*plan);
  BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "limit plan have exactly one child");
  size_t n = limit_plan.GetLimit();

  auto child_plan = optimized_plan->GetChildAt(0);
  if (child_plan->GetType() != PlanType::Sort) {
    return optimized_plan;
  }
  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
  const auto &order_bys = sort_plan.GetOrderBy();

  return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildAt(0), order_bys, n);
}

}  // namespace bustub
