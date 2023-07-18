//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tuple{};
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
  right_cursor_ = right_tuples_.size();
  has_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();

  while (true) {
    if (right_cursor_ == right_tuples_.size()) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }

      right_cursor_ = 0;
      has_matched_ = false;
    }

    while (right_cursor_ < right_tuples_.size()) {
      auto &right_tuple = right_tuples_[right_cursor_++];
      auto join_predicate = plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
      auto matched = !join_predicate.IsNull() && join_predicate.GetAs<bool>();
      if (!matched) {
        continue;
      }
      has_matched_ = true;

      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.emplace_back(right_tuple.GetValue(&right_schema, i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !has_matched_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

}  // namespace bustub
