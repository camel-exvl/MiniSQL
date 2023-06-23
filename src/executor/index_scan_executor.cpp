#include "executor/executors/index_scan_executor.h"

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  TableInfo *table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info);
  table_heap_ = table_info->GetTableHeap();

  queue<AbstractExpressionRef> predicates;
  predicates.push(plan_->GetPredicate());
  while (!predicates.empty()) {
    auto predicate = predicates.front();
    predicates.pop();
    if (predicate->GetType() == ExpressionType::ComparisonExpression) {
      std::string compare_operator = std::dynamic_pointer_cast<ComparisonExpression>(predicate)->GetComparisonType();
      IndexInfo *index_info = nullptr;
      for (auto index : plan_->indexes_) {
        if (index->GetIndexKeySchema()->GetColumn(0)->GetName() == table_info->GetSchema()->GetColumn(std::dynamic_pointer_cast<ColumnValueExpression>(predicate->GetChildAt(0))->GetColIdx())->GetName()) {
          index_info = index;
          break;
        }
      }
      assert(index_info != nullptr);
      vector<RowId> cur_row_id;
      vector<Field> field;
      field.emplace_back((std::dynamic_pointer_cast<ConstantValueExpression>(predicate->GetChildAt(1)))->val_);
      Row key(field);
      index_info->GetIndex()->ScanKey(key, cur_row_id, exec_ctx_->GetTransaction(), compare_operator);
      if (row_ids_.empty()) {
        row_ids_ = cur_row_id;
      } else {
        std::set_intersection(row_ids_.begin(), row_ids_.end(), cur_row_id.begin(), cur_row_id.end(), back_inserter(row_ids_), [](const RowId &a, const RowId &b) { return a.GetPageId() == b.GetPageId() ? a.GetSlotNum() < b.GetSlotNum() : a.GetPageId() < b.GetPageId(); });
      }
    }
    for (auto child : predicate->GetChildren()) {
      predicates.push(child);
    }
  }
  cur_row_id_ = 0;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if (cur_row_id_ >= row_ids_.size()) {
    return false;
  }
  *rid = row_ids_[cur_row_id_];
  row->SetRowId(*rid);
  if (!table_heap_->GetTuple(row, exec_ctx_->GetTransaction())) {
    return false;
  }
  cur_row_id_++;
  return true;
}
