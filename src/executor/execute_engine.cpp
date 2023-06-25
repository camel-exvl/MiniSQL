#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  std::string db_name = ast->child_->val_;
  // check if database already exists
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  // create database
  DBStorageEngine *db = new DBStorageEngine(db_name, true);
  dbs_[db_name] = db;
  printf("Database %s created.\n", db_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  std::string db_name = ast->child_->val_;
  // check if database exists
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  // drop database
  DBStorageEngine *db = dbs_[db_name];
  dbs_.erase(db_name);
  remove(db_name.c_str());
  delete db;
  if (current_db_ == db_name) {
    current_db_.clear();
  }
  printf("Database %s dropped.\n", db_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  int max_width = 8;
  for (const auto &db : dbs_) {
    max_width = max(max_width, int(db.first.length()));
  }
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  printf("|");
  printf(" %s%*s", "Database", max_width - 8, " ");
  if (max_width - 8 > 0) {
    printf(" ");
  }
  printf("|\n");
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  for (const auto &db : dbs_) {
    printf("|");
    printf(" %s%*s", db.first.c_str(), max_width - int(db.first.length()), " ");
    if (max_width - int(db.first.length()) > 0) {
      printf(" ");
    }
    printf("|\n");
  }
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  std::string db_name = ast->child_->val_;
  // check if database exists
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  // use database
  current_db_ = db_name;
  printf("Database changed to %s.\n", db_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto iter = dbs_.find(current_db_);
  if (iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = iter->second;
  std::vector<std::string> tables;
  if (db->catalog_mgr_->GetTableNames(tables) != DB_SUCCESS) {
    return DB_FAILED;
  }

  int max_width = 10 + current_db_.length();
  for (const auto &table : tables) {
    max_width = max(max_width, int(table.length()));
  }
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  printf("|");
  printf(" %s%s%*s", "Tables_in_", current_db_.c_str(), max_width - 10 - int(current_db_.length()), " ");
  if (max_width - 10 - int(current_db_.length()) > 0) {
    printf(" ");
  }
  printf("|\n");
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  for (const auto &table : tables) {
    printf("|");
    printf(" %s%*s", table.c_str(), max_width - int(table.length()), " ");
    if (max_width - int(table.length()) > 0) {
      printf(" ");
    }
    printf("|\n");
  }
  printf("+");
  for (int i = 0; i < max_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto storage_engine_iter = dbs_.find(current_db_);
  if (storage_engine_iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = storage_engine_iter->second;
  std::string table_name = ast->child_->val_;

  // get table schema
  std::vector<Column *> columns;
  uint32_t index = 0;
  std::unordered_map<std::string, std::vector<std::string>> column_constraints;
  for(pSyntaxNode node = ast->child_->next_->child_; node != nullptr; node = node->next_) {
    if (node->val_ != nullptr && std::string(node->val_) == "primary keys") {
      std::vector<std::string> keys;
      // set nullable and unique
      for (pSyntaxNode key_node = node->child_; key_node != nullptr; key_node = key_node->next_) {
        std::string key_name = key_node->val_;
        keys.emplace_back(key_name);
        for (auto column : columns) {
          if (column->GetName() == key_name) {
            column->SetNullable(false);
            column->SetUnique(true);
            break;
          }
        }
      }
      // create index
      std::string index_name = table_name + "_index_" + std::to_string(time(nullptr));
      column_constraints[index_name] = keys;
    } else {
      std::string column_name = node->child_->val_;
      TypeId type = TypeId::kTypeInvalid;
      uint32_t length = 0;
      bool nullable = true;
      bool unique = false;

      if (node->val_ != nullptr && std::string(node->val_) == "unique") {
        unique = true;
        std::string index_name = table_name + "_index_" + column_name + "_unique";
        column_constraints[index_name] = {column_name};
      }
      std::string type_str = node->child_->next_->val_;
      if (type_str == "int") {
        type = TypeId::kTypeInt;
      } else if (type_str == "char") {
        type = TypeId::kTypeChar;
        length = std::stoi(node->child_->next_->child_->val_);
      } else if (type_str == "float") {
        type = TypeId::kTypeFloat;
      } else {
        return DB_FAILED;
      }

      if (type == TypeId::kTypeChar) {
        columns.push_back(new Column(column_name, type, length, index, nullable, unique));
      } else {
        columns.push_back(new Column(column_name, type, index, nullable, unique));
      }
      index++;
    }
  }
  TableSchema schema(columns);
  TableInfo *table_info;
  dberr_t res =db->catalog_mgr_->CreateTable(table_name, &schema, context->GetTransaction(), table_info);
  if (res != DB_SUCCESS) {
    return res;
  }
  // create index
  for (auto &constraint : column_constraints) {
    IndexInfo *index_info;
    if (db->catalog_mgr_->CreateIndex(table_name, constraint.first, constraint.second, context->GetTransaction(), index_info, "bptree") != DB_SUCCESS) {
      return res;
    }
  }
  printf("Table %s created.\n", table_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto storage_engine_iter = dbs_.find(current_db_);
  if (storage_engine_iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = storage_engine_iter->second;
  std::string table_name = ast->child_->val_;
  dberr_t res = db->catalog_mgr_->DropTable(table_name);
  if (res != DB_SUCCESS) {
    return res;
  }
  printf("Table %s dropped.\n", table_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto storage_engine_iter = dbs_.find(current_db_);
  if (storage_engine_iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = storage_engine_iter->second;
  std::vector<std::string> tables;
  if (db->catalog_mgr_->GetTableNames(tables) != DB_SUCCESS) {
    return DB_FAILED;
  }

  std::vector<IndexInfo *> indexes;
  for (const auto &table : tables) {
    std::string table_name = table;
    std::vector<IndexInfo *> index;
    dberr_t res = db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    if (res != DB_SUCCESS) {
      if (res == DB_INDEX_NOT_FOUND) {
        continue;
      }
      return res;
    }
    indexes.insert(indexes.end(), index.begin(), index.end());
  }

  int max_table_name_width = 10;
  int max_column_name_width = 11;
  int max_index_name_width = 10;
  for (const auto &table : tables) {
    max_table_name_width = max(max_table_name_width, int(table.length()));
  }
  // TODO: 或许可以显示一下表名
  for (auto index : indexes) {
    max_index_name_width = std::max(max_index_name_width, int(index->GetIndexName().length()));
  }
  vector<string> index_columns;
  for (auto index : indexes) {
    std::vector<Column *> columns = index->GetIndexKeySchema()->GetColumns();
    std::string column_name = columns[0]->GetName();
    for (int i = 1; i < columns.size(); i++) {
      column_name += ", " + columns[i]->GetName();
    }
    max_column_name_width = std::max(max_column_name_width, int(column_name.length()));
    index_columns.push_back(column_name);
  }
  printf("+");
  for (int i = 0; i < max_index_name_width + 2; i++) {
    printf("-");
  }
  printf("+");
  for (int i = 0; i < max_column_name_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  // printf("| %s%*s", "Table_name", max_table_name_width - 10, " ");
  // if (max_table_name_width - 10 > 0) {
  //   printf(" ");
  // }
  printf("| %s%*s", "Index_name", max_index_name_width - 10, " ");
  if (max_index_name_width - 10 > 0) {
    printf(" ");
  }
  printf("| %s%*s", "Column_name", max_column_name_width - 11, " ");
  if (max_column_name_width - 11 > 0) {
    printf(" ");
  }
  printf("|\n+");
  for (int i = 0; i < max_index_name_width + 2; i++) {
    printf("-");
  }
  printf("+");
  for (int i = 0; i < max_column_name_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  for (int i = 0; i < indexes.size(); i++) {
    printf("| %s%*s", indexes[i]->GetIndexName().c_str(), max_index_name_width - indexes[i]->GetIndexName().length(), " ");
    if (max_index_name_width - indexes[i]->GetIndexName().length() > 0) {
      printf(" ");
    }
    printf("| %s%*s", index_columns[i].c_str(), max_column_name_width - index_columns[i].length(), " ");
    if (max_column_name_width - index_columns[i].length() > 0) {
      printf(" ");
    }
    printf("|\n");
  }
  printf("+");
  for (int i = 0; i < max_index_name_width + 2; i++) {
    printf("-");
  }
  printf("+");
  for (int i = 0; i < max_column_name_width + 2; i++) {
    printf("-");
  }
  printf("+\n");
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto storage_engine_iter = dbs_.find(current_db_);
  if (storage_engine_iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = storage_engine_iter->second;
  std::string index_name = ast->child_->val_;
  std::string table_name = ast->child_->next_->val_;
  std::string index_type = "bptree";
  std::vector<std::string> keys;
  if (std::string(ast->child_->next_->next_->val_) != "index keys") {
    return DB_FAILED;
  }
  for (pSyntaxNode node = ast->child_->next_->next_->child_; node != nullptr; node = node->next_) {
    keys.emplace_back(node->val_);
  }
  if (ast->child_->next_->next_->next_ != nullptr) {
    if (std::string(ast->child_->next_->next_->next_->val_) != "index type") {
      return DB_FAILED;
    }
    index_type = ast->child_->next_->next_->next_->child_->val_;
  }
  IndexInfo *index_info;
  dberr_t res = db->catalog_mgr_->CreateIndex(table_name, index_name, keys, context->GetTransaction(), index_info, index_type);
  if (res != DB_SUCCESS) {
    return res;
  }

  // insert the tuples into the index
  TableInfo *table_info;
  res = db->catalog_mgr_->GetTable(table_name, table_info);
  if (res != DB_SUCCESS) {
    return res;
  }
  TableHeap *table_heap = table_info->GetTableHeap();
  for (auto tuple = table_heap->Begin(context->GetTransaction()); tuple != table_heap->End(); tuple++) {
    std::vector<Field> fields;
    for (int i = 0; i < keys.size(); i++) {
      uint32_t column_index;
      if (table_info->GetSchema()->GetColumnIndex(keys[i], column_index) != DB_SUCCESS) {
        return DB_FAILED;
      }
      fields.emplace_back(*(tuple->GetField(column_index)));
    }
    index_info->GetIndex()->InsertEntry(Row(fields), tuple->GetRowId(), context->GetTransaction());
  }
  printf("Create index %s on table %s success.\n", index_name.c_str(), table_name.c_str());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto storage_engine_iter = dbs_.find(current_db_);
  if (storage_engine_iter == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  DBStorageEngine *db = storage_engine_iter->second;
  std::string index_name = ast->child_->val_;
  std::vector<std::string> table_names;
  if (db->catalog_mgr_->GetTableNames(table_names) != DB_SUCCESS) {
    return DB_FAILED;
  }

  for (auto table_name : table_names) {
    IndexInfo *index_info;
    dberr_t res = db->catalog_mgr_->GetIndex(table_name, index_name, index_info);
    if (res != DB_SUCCESS) {
      if (res == DB_INDEX_NOT_FOUND) {
        continue;
      }
      return DB_FAILED;
    }
    if (db->catalog_mgr_->DropIndex(table_name, index_name) != DB_SUCCESS) {
      return DB_FAILED;
    }

    if (db->catalog_mgr_->DeleteIndex(table_name, index_name) != DB_SUCCESS) {
      return DB_FAILED;
    }

    printf("Drop index %s on table %s success.\n", index_name.c_str(), table_name.c_str());
    return DB_SUCCESS;
  }
  return DB_INDEX_NOT_FOUND;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  std::string file_name = ast->child_->val_;
  std::ifstream file;
  file.open(file_name);
  if (!file) {
    printf("Failed to open file %s.\n", file_name.c_str());
    return DB_FAILED;
  }

  auto start_time = std::chrono::system_clock::now();

  const int buf_size = 1024;
  char buffer[buf_size] = {0};
  char ch;
  int i = 0;
  // executor engine
  // ExecuteEngine engine;
  // for print syntax tree
  // TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  // uint32_t syntax_tree_id = 0;

  while (file.get(ch)) {
    if (ch == ';') {
      buffer[i] = ch;
      // LOG(INFO) << "Execute: " << buffer << std::endl;

      // create buffer for sql input
      YY_BUFFER_STATE bp = yy_scan_string(buffer);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();

      // parse result handle
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      } else {
        // Comment them out if you don't need to debug the syntax tree
        // printf("[INFO] Sql syntax parse ok!\n");
        // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
        // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
      }

      auto result = Execute(MinisqlGetParserRootNode());

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      // quit condition
      ExecuteInformation(result);
      if (result == DB_QUIT) {
        break;
      }

      memset(buffer, 0, sizeof(buffer));
      i = 0;
    } else if (ch != '\n' && ch != '\r') {
      buffer[i++] = ch;
      if (i == sizeof(buffer)) {
        printf("Buffer overflow.\n");
        return DB_FAILED;
      }
    }
  }

  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);
  printf("Execute file %s success.\n", file_name.c_str());
  printf("Total time: %.4lf sec\n", duration_time / 1000);
  file.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
