//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  TableInfo *table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info);
  table_iter_ = new TableIterator(table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction()));
  end_iter_ = new TableIterator(table_info->GetTableHeap()->End());
  schema_ = table_info->GetSchema();
  key_schema_ = plan_->OutputSchema();
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  if (*table_iter_ == *end_iter_) {
    return false;
  }
  if (plan_->GetPredicate() == nullptr) { // no predicate
    Row row_src = **table_iter_;
    Row row_dst;
    row_src.GetKeyFromRow(schema_, key_schema_, row_dst);
    *row = row_dst;
    *rid = (*table_iter_)->GetRowId();
    (*table_iter_)++;
    return true;
  }
  for (; *table_iter_ != *end_iter_; (*table_iter_)++) {
    *row = **table_iter_;
    *rid = (*table_iter_)->GetRowId();
    if (plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1))) {
      (*table_iter_)++;
      Row row_src = *row;
      Row row_dst;
      row_src.GetKeyFromRow(schema_, key_schema_, row_dst);
      *row = row_dst;
      return true;
    }
  }
  return false;
}
