#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    if (row.GetSerializedSize(schema_) >= PAGE_SIZE) {
        LOG(ERROR) << "row tuple size is too large!";
        return false;
    }

    page_id_t page_id = first_page_id_;
    for (;;) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        page->WLatch();
        if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, true);
            return true;
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
        if (page->GetNextPageId() == INVALID_PAGE_ID) {
            break;
        }
        page_id = page->GetNextPageId();
    }

    // If no page can fit the tuple, then create a new page.
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    assert(new_page != nullptr);
    new_page->WLatch();
    new_page->Init(new_page_id, page_id, log_manager_, txn);
    new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    old_page->WLatch();
    old_page->SetNextPageId(new_page_id);
    old_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
    if (row.GetSerializedSize(schema_) >= PAGE_SIZE) {
        LOG(ERROR) << "row tuple size is too large!";
        return false;
    }
    auto *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    page->WLatch();
    Row old_row(rid);
    if (!page->GetTuple(&old_row, schema_, txn, lock_manager_)) {
        LOG(ERROR) << "old page get tuple failed!";
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
        return false;
    }
    bool space_enough;
    bool result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_, &space_enough);
    if (!space_enough) {
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
        bool deleteResult = MarkDelete(rid, txn);
        if (!deleteResult) {
            LOG(ERROR) << "delete failed!";
            return false;
        }
        bool insertResult = InsertTuple(const_cast<Row &>(row), txn);
        if (!insertResult) {
            LOG(ERROR) << "insert failed!";
            return false;
        }
        return true;
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), result);
    return result;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
    // Step1: Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    assert(page != nullptr);
    page->RLatch();
    bool result = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page =
            reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } else {
        DeleteTable(first_page_id_);
    }
}

TableIterator TableHeap::Begin(Transaction *txn) {
    return TableIterator(BEGIN_ITERATOR, first_page_id_, schema_, buffer_pool_manager_, txn, log_manager_, lock_manager_);
}

TableIterator TableHeap::End(Transaction *txn) {
    return TableIterator(END_ITERATOR, first_page_id_, schema_, buffer_pool_manager_, txn, log_manager_, lock_manager_);
}
