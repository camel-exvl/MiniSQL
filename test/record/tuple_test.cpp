#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest) {
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    // Serialize phase
    char *p = buffer;
    for (int i = 0; i < 4; i++) {
        p += int_fields[i].SerializeTo(p);
    }
    for (int i = 0; i < 3; i++) {
        p += float_fields[i].SerializeTo(p);
    }
    for (int i = 0; i < 4; i++) {
        p += char_fields[i].SerializeTo(p);
    }
    // Deserialize phase
    uint32_t ofs = 0;
    Field *df = nullptr;
    for (int i = 0; i < 4; i++) {
        ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
        EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
        EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
        EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
        delete df;
        df = nullptr;
    }
    for (int i = 0; i < 3; i++) {
        ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
        EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
        EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
        EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
        delete df;
        df = nullptr;
    }
    for (int i = 0; i < 3; i++) {
        ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
        EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
        EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
        EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
        EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
        delete df;
        df = nullptr;
    }
}

TEST(TupleTest, RowSerializeDeserializeTest) {
    // Create schema
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                                 Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                                 Field(TypeId::kTypeFloat, 19.99f)};
    auto schema = std::make_shared<Schema>(columns);
    Row row(fields);
    // Serialize phase
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    char *p = buffer;
    p += row.SerializeTo(p, schema.get());
    EXPECT_EQ(p - buffer, row.GetSerializedSize(schema.get()));
    // Deserialize phase
    Row deserialized_row;
    uint32_t ofs = 0;
    ofs += deserialized_row.DeserializeFrom(buffer + ofs, schema.get());
    EXPECT_EQ(ofs, row.GetSerializedSize(schema.get()));
    EXPECT_EQ(CmpBool::kTrue, deserialized_row.GetRowId() == row.GetRowId());
    EXPECT_EQ(CmpBool::kTrue, deserialized_row.GetField(0)->CompareEquals(fields[0]));
    EXPECT_EQ(CmpBool::kTrue, deserialized_row.GetField(1)->CompareEquals(fields[1]));
    EXPECT_EQ(CmpBool::kTrue, deserialized_row.GetField(2)->CompareEquals(fields[2]));

    // Test null fields
    std::vector<Field> null_fields = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};
    Row null_row(null_fields);
    memset(buffer, 0, sizeof(buffer));
    p = buffer;
    p += null_row.SerializeTo(p, schema.get());
    EXPECT_EQ(p - buffer, null_row.GetSerializedSize(schema.get()));
    Row deserialized_null_row;
    ofs = 0;
    ofs += deserialized_null_row.DeserializeFrom(buffer + ofs, schema.get());
    EXPECT_EQ(ofs, null_row.GetSerializedSize(schema.get()));
    EXPECT_EQ(CmpBool::kTrue, deserialized_null_row.GetRowId() == null_row.GetRowId());
    EXPECT_EQ(CmpBool::kTrue, deserialized_null_row.GetField(0)->IsNull());
    EXPECT_EQ(CmpBool::kTrue, deserialized_null_row.GetField(1)->IsNull());
    EXPECT_EQ(CmpBool::kTrue, deserialized_null_row.GetField(2)->IsNull());

    // Test empty row
    auto empty_schema = std::make_shared<Schema>(std::vector<Column *>());
    Row empty_row;
    memset(buffer, 0, sizeof(buffer));
    p = buffer;
    p += empty_row.SerializeTo(p, empty_schema.get());
    EXPECT_EQ(p - buffer, empty_row.GetSerializedSize(empty_schema.get()));
    Row deserialized_empty_row;
    ofs = 0;
    ofs += deserialized_empty_row.DeserializeFrom(buffer + ofs, empty_schema.get());
    EXPECT_EQ(ofs, empty_row.GetSerializedSize(empty_schema.get()));
    EXPECT_EQ(CmpBool::kTrue, deserialized_empty_row.GetRowId() == empty_row.GetRowId());
    EXPECT_EQ(CmpBool::kTrue, deserialized_empty_row.GetFields().empty());
}

TEST(TupleTest, ColumnSerializeDeserializeTest) {
    // Create column
    std::vector<Column> columns = {Column("id", TypeId::kTypeInt, 0, false, false),
                                   Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   Column("account", TypeId::kTypeFloat, 2, true, false)};
    // Serialize phase
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    char *p = buffer, *pre = buffer;
    for (auto &column : columns) {
        pre = p;
        p += column.SerializeTo(p);
        EXPECT_EQ(p - pre, column.GetSerializedSize());
    }
    // Deserialize phase
    std::vector<Column *> deserialized_columns;
    uint32_t ofs = 0 , pre_ofs = 0;
    for (int i = 0; i < columns.size(); ++i) {
        Column *column = nullptr;
        pre_ofs = ofs;
        ofs += column->DeserializeFrom(buffer + ofs, column);
        EXPECT_EQ(ofs - pre_ofs, column->GetSerializedSize());
        deserialized_columns.push_back(column);
    }
    EXPECT_EQ(columns.size(), deserialized_columns.size());
    for (int i = 0; i < columns.size(); ++i) {
        EXPECT_EQ(CmpBool::kTrue, columns[i].GetName().compare(deserialized_columns[i]->GetName()) == 0);
        EXPECT_EQ(columns[i].GetType(), deserialized_columns[i]->GetType());
        EXPECT_EQ(columns[i].GetLength(), deserialized_columns[i]->GetLength());
        EXPECT_EQ(columns[i].GetTableInd(), deserialized_columns[i]->GetTableInd());
        EXPECT_EQ(columns[i].IsNullable(), deserialized_columns[i]->IsNullable());
        EXPECT_EQ(columns[i].IsUnique(), deserialized_columns[i]->IsUnique());
        delete deserialized_columns[i];
    }
}

TEST(TupleTest, SchemaSerializeDeserializeTest) {
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    auto schema = std::make_shared<Schema>(columns);
    // Serialize phase
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    char *p = buffer;
    p += schema->SerializeTo(p);
    EXPECT_EQ(p - buffer, schema->GetSerializedSize());
    // Deserialize phase
    Schema *deserialized_schema = nullptr;
    uint32_t ofs = 0;
    ofs += deserialized_schema->DeserializeFrom(buffer + ofs, deserialized_schema);
    EXPECT_EQ(ofs, schema->GetSerializedSize());
    for (int i = 0; i < columns.size(); ++i) {
        EXPECT_EQ(CmpBool::kTrue, schema->GetColumn(i)->GetName().compare(deserialized_schema->GetColumn(i)->GetName()) == 0);
        EXPECT_EQ(schema->GetColumn(i)->GetType(), deserialized_schema->GetColumn(i)->GetType());
        EXPECT_EQ(schema->GetColumn(i)->GetLength(), deserialized_schema->GetColumn(i)->GetLength());
        EXPECT_EQ(schema->GetColumn(i)->GetTableInd(), deserialized_schema->GetColumn(i)->GetTableInd());
        EXPECT_EQ(schema->GetColumn(i)->IsNullable(), deserialized_schema->GetColumn(i)->IsNullable());
        EXPECT_EQ(schema->GetColumn(i)->IsUnique(), deserialized_schema->GetColumn(i)->IsUnique());
    }
    delete deserialized_schema;
}

TEST(TupleTest, RowTest) {
    TablePage table_page;
    // create schema
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                                 Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                                 Field(TypeId::kTypeFloat, 19.99f)};
    auto schema = std::make_shared<Schema>(columns);
    Row row(fields);
    table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
    table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
    RowId first_tuple_rid;
    ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
    ASSERT_EQ(row.GetRowId(), first_tuple_rid);
    Row row2(row.GetRowId());
    ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
    std::vector<Field *> &row2_fields = row2.GetFields();
    ASSERT_EQ(3, row2_fields.size());
    for (size_t i = 0; i < row2_fields.size(); i++) {
        ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
    }
    ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
    table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}