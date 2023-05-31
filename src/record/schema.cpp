#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
    uint32_t offset = 0;
    MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
    offset += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(columns_.size()));
    offset += sizeof(uint32_t);
    for (auto &column : columns_) {
        offset += column->SerializeTo(buf + offset);
    }
    MACH_WRITE_UINT32(buf + offset, is_manage_);
    offset += sizeof(uint32_t);
    return offset;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t size = sizeof(uint32_t) * 2;
    for (auto &column : columns_) {
        size += column->GetSerializedSize();
    }
    size += sizeof(uint32_t);
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    if (schema != nullptr) {
        LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
        delete schema;
    }
    uint32_t offset = 0;
    uint32_t magic = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    ASSERT(magic == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");
    uint32_t numColumns = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    std::vector<Column *> columns;
    for (uint32_t i = 0; i < numColumns; ++i) {
        Column *column;
        offset += Column::DeserializeFrom(buf + offset, column);
        columns.push_back(column);
    }
    bool isManage = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    schema = new Schema(columns, isManage);
    return offset;
}