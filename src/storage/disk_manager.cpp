#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  page_id_t logical_page_id = INVALID_PAGE_ID;
  for (page_id_t i = 0; i < reinterpret_cast<DiskFileMetaPage *>(meta_data_)->GetExtentNums(); i++) {
    if (reinterpret_cast<DiskFileMetaPage *>(meta_data_)->GetExtentUsedPage(i) < BITMAP_SIZE) {
      logical_page_id = i * BITMAP_SIZE + reinterpret_cast<DiskFileMetaPage *>(meta_data_)->GetExtentUsedPage(i);
      break;
    }
  }
  if (logical_page_id == INVALID_PAGE_ID) {
    // no free page, allocate a new extent
    page_id_t extent_id = reinterpret_cast<DiskFileMetaPage *>(meta_data_)->GetExtentNums();

    reinterpret_cast<DiskFileMetaPage *>(meta_data_)->num_extents_++;
    reinterpret_cast<DiskFileMetaPage *>(meta_data_)->extent_used_page_[extent_id] = 0;

    char bitmap_page[PAGE_SIZE];
    memset(bitmap_page, 0, PAGE_SIZE);
    uint32_t page_offset;
    if (reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page)->AllocatePage(page_offset)) {
      reinterpret_cast<DiskFileMetaPage *>(meta_data_)->num_allocated_pages_++;
      reinterpret_cast<DiskFileMetaPage *>(meta_data_)->extent_used_page_[extent_id]++;
      logical_page_id = extent_id * BITMAP_SIZE + page_offset;
      page_id_t bitmap_physical_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
      WritePhysicalPage(META_PAGE_ID, meta_data_);
      WritePhysicalPage(bitmap_physical_page_id, bitmap_page);
    } else {
      throw std::runtime_error("Allocate page failed.");
    }
  } else {
    // allocate a new page in an existing extent
    page_id_t physical_page_id = MapPageId(logical_page_id);
    page_id_t bitmap_id = (physical_page_id - 1) / (BITMAP_SIZE + 1);
    page_id_t bitmap_physical_page_id = bitmap_id * (BITMAP_SIZE + 1) + 1;
    uint32_t page_offset;
    char bitmap_page[PAGE_SIZE];
    ReadPhysicalPage(bitmap_physical_page_id, bitmap_page);
    reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page)->AllocatePage(page_offset);
    reinterpret_cast<DiskFileMetaPage *>(meta_data_)->num_allocated_pages_++;
    reinterpret_cast<DiskFileMetaPage *>(meta_data_)->extent_used_page_[bitmap_id]++;
    WritePhysicalPage(bitmap_physical_page_id, bitmap_page);
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    physical_page_id = bitmap_physical_page_id + page_offset + 1;
    // convert to logical id
    page_id_t index = (physical_page_id - 1) / (BITMAP_SIZE + 1);
    page_id_t offset = (physical_page_id - 1) % (BITMAP_SIZE + 1) - 1;
    logical_page_id = index * BITMAP_SIZE + offset;
  }
  return logical_page_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  page_id_t physical_page_id = MapPageId(logical_page_id);
  page_id_t bitmap_id = (physical_page_id - 1) / (BITMAP_SIZE + 1);
  page_id_t offset = (physical_page_id - 1) % (BITMAP_SIZE + 1) - 1;
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(bitmap_id, bitmap_page);
  reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page)->DeAllocatePage(offset);
  reinterpret_cast<DiskFileMetaPage *>(meta_data_)->num_allocated_pages_--;
  reinterpret_cast<DiskFileMetaPage *>(meta_data_)->extent_used_page_[bitmap_id]--;
  WritePhysicalPage(bitmap_id, bitmap_page);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  page_id_t physical_page_id = MapPageId(logical_page_id);
  page_id_t bitmap_page_id = physical_page_id / (BITMAP_SIZE + 1);
  page_id_t offset = (physical_page_id - 1) % (BITMAP_SIZE + 1);
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(bitmap_page_id, bitmap_page);
  return reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page)->IsPageFree(offset);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t index = logical_page_id / BITMAP_SIZE;
  page_id_t offset = logical_page_id % BITMAP_SIZE;
  return index * (BITMAP_SIZE + 1) + offset + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}