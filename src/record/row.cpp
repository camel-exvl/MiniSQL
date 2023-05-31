#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    if (fields_.empty()) {
        return 0;
    }
    uint32_t offset = 0;
    page_id_t page_id = rid_.GetPageId();
    uint32_t slot_num = rid_.GetSlotNum();
    MACH_WRITE_UINT32(buf + offset, page_id);
    offset += sizeof(page_id_t);
    MACH_WRITE_INT32(buf + offset, slot_num);
    offset += sizeof(uint32_t);

    // Field Nums
    MACH_WRITE_UINT32(buf + offset, fields_.size());
    offset += sizeof(uint32_t);

    // Null bitmap
    uint32_t nullBitmapSize = fields_.size() / 8 + (fields_.size() % 8 == 0 ? 0 : 1);
    char *nullBitmap = new char[nullBitmapSize];
    memset(nullBitmap, 0, nullBitmapSize);
    for (uint32_t i = 0; i < fields_.size(); i++) {
        if (fields_[i]->IsNull()) {
            nullBitmap[i / 8] |= (1 << (i % 8));
        }
    }
    memcpy(buf + offset, &nullBitmapSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(buf + offset, nullBitmap, nullBitmapSize);
    offset += nullBitmapSize;
    delete[] nullBitmap;

    for (uint32_t i = 0; i < fields_.size(); i++) {
        if (!fields_[i]->IsNull()) {
            offset += fields_[i]->SerializeTo(buf + offset);
        }
    }
    return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    if (!fields_.empty()) {
        LOG(WARNING) << "This row is not empty." << std::endl;
        fields_.clear();
    }
    uint32_t offset = 0;
    page_id_t page_id;
    uint32_t slot_num;
    page_id = MACH_READ_UINT32(buf + offset);
    offset += sizeof(page_id_t);
    slot_num = MACH_READ_INT32(buf + offset);
    offset += sizeof(uint32_t);
    rid_ = RowId(page_id, slot_num);

    // Field Nums
    uint32_t fieldNums;
    fieldNums = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);

    // Null bitmap
    uint32_t nullBitmapSize;
    nullBitmapSize = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    char *nullBitmap = new char[nullBitmapSize];
    memcpy(nullBitmap, buf + offset, nullBitmapSize);
    offset += nullBitmapSize;

    for (uint32_t i = 0; i < fieldNums; i++) {
        TypeId type = schema->GetColumn(i)->GetType();
        if (nullBitmap[i / 8] & (1 << (i % 8))) {
            fields_.push_back(new Field(type));
        } else {
            Field *field = new Field(type);
            offset += field->DeserializeFrom(buf + offset, type, &field, false);
        }
    }
    delete[] nullBitmap;
    return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    if (fields_.empty()) {
        return 0;
    }
    uint32_t size = 0;
    size += sizeof(page_id_t);
    size += sizeof(uint32_t);

    // Field Nums
    size += sizeof(uint32_t);

    // Null bitmap
    uint32_t nullBitmapSize = fields_.size() / 8 + (fields_.size() % 8 == 0 ? 0 : 1);
    size += sizeof(uint32_t);
    size += nullBitmapSize;

    for (uint32_t i = 0; i < fields_.size(); i++) {
        if (!fields_[i]->IsNull()) {
            size += fields_[i]->GetSerializedSize();
        }
    }
    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column : columns) {
        schema->GetColumnIndex(column->GetName(), idx);
        fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}
