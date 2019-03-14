#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Storages/ColumnDefault.h>
#include <Core/Block.h>
#include <Storages/ColumnCodec.h>


namespace DB
{

struct ColumnDescription
{
    ColumnDescription() = default;
    ColumnDescription(String name_, DataTypePtr type_) : name(std::move(name_)), type(std::move(type_)) {}

    String name;
    DataTypePtr type;
    ColumnDefault default_desc;
    String comment;
    CompressionCodecPtr codec;

    bool operator==(const ColumnDescription & other) const;
    bool operator!=(const ColumnDescription & other) const { return !(*this == other); }

    void writeText(WriteBuffer & buf) const;
    void readText(ReadBuffer & buf);
};

struct ColumnsDescription
{
private:
    std::list<ColumnDescription> columns;
    std::unordered_map<String, std::list<ColumnDescription>::iterator> name_to_column;

public:
    ColumnsDescription() = default;
    explicit ColumnsDescription(NamesAndTypesList ordinary_);

    void add(ColumnDescription column)
    {
        /// TODO: check that the column doesn't exist.
        auto it = columns.insert(columns.end(), std::move(column));
        name_to_column.emplace(it->name, it);
    }

    void flattenNested();

    bool operator==(const ColumnsDescription & other) const { return columns == other.columns; }
    bool operator!=(const ColumnsDescription & other) const { return !(*this == other); }

    std::list<ColumnDescription>::const_iterator begin() const { return columns.begin(); }
    std::list<ColumnDescription>::const_iterator end() const { return columns.end(); }

    NamesAndTypesList getOrdinary() const;

    NamesAndTypesList getMaterialized() const;

    NamesAndTypesList getAliases() const;

    /// ordinary + materialized.
    NamesAndTypesList getAllPhysical() const;

    /// ordinary + materialized + aliases.
    NamesAndTypesList getAll() const;

    Names getNamesOfPhysical() const;

    NameAndTypePair getPhysical(const String & column_name) const;

    bool hasPhysical(const String & column_name) const;

    ColumnDefaults getDefaults() const; /// TODO: remove

    CompressionCodecPtr getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;

    CompressionCodecPtr getCodecOrDefault(const String & column_name) const;

    String toString() const;
    static ColumnsDescription parse(const String & str);

    static const ColumnsDescription * loadFromContext(const Context & context, const String & db, const String & table);
};

}
