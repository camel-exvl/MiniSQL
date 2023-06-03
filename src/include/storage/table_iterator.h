#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

#include "buffer/buffer_pool_manager.h"
#include "page/header_page.h"
#include "page/table_page.h"
#include "transaction/lock_manager.h"
#include "transaction/log_manager.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(bool mode, page_id_t first_page_id, Schema *schema, BufferPoolManager *buffer_pool_manager, Transaction *txn, LogManager *log_manager, LockManager *lock_manager);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  Row *row_;
  TablePage *page_;
  Schema *schema_;
  BufferPoolManager *buffer_pool_manager_;
  Transaction *txn_;
  [[maybe_unused]] LogManager *log_manager_;
  [[maybe_unused]] LockManager *lock_manager_;
};

#endif  // MINISQL_TABLE_ITERATOR_H
