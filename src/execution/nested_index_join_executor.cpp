//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  right_table_ = catalog->GetTable(tree_->GetMetadata()->GetTableName());
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  result_cursor_ = 0;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = child_executor_->GetOutputSchema();
  Tuple left_tuple{};
  auto right_schema = right_table_->schema_;
  Tuple right_tuple{};

  while (true) {
    if (result_cursor_ == result_.size()) {
      if (!child_executor_->Next(&left_tuple, rid)) {
        return false;
      }

      Tuple index_key = Tuple{{plan_->key_predicate_->Evaluate(&left_tuple, left_schema)}, tree_->GetKeySchema()};
      result_.clear();
      tree_->ScanKey(index_key, &result_, exec_ctx_->GetTransaction());
      result_cursor_ = 0;
    }

    if (plan_->GetJoinType() == JoinType::LEFT && result_.empty()) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }

    while (result_cursor_ < result_.size()) {
      right_table_->table_->GetTuple(result_[result_cursor_++], &right_tuple, exec_ctx_->GetTransaction());

      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.emplace_back(right_tuple.GetValue(&right_schema, i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

}  // namespace bustub
