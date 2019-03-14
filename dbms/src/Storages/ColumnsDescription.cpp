#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionFactory.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_PARSE_TEXT;
}

bool ColumnDescription::operator==(const ColumnDescription & other) const
{
    return name == other.name
        && type->equals(*other.type)
        && default_desc == other.default_desc
        && comment == other.comment
        && codec->getCodecDesc() == other.codec->getCodecDesc();
}

void ColumnDescription::writeText(WriteBuffer & buf) const
{
    writeBackQuotedString(name, buf);
    writeChar(' ', buf);
    DB::writeText(type->getName(), buf);

    if (default_desc.expression)
    {
        writeChar('\t', buf);
        DB::writeText(DB::toString(default_desc.kind), buf);
        writeChar('\t', buf);
        DB::writeText(queryToString(default_desc.expression), buf);
    }

    if (!comment.empty())
    {
        writeChar('\t', buf);
        DB::writeText("COMMENT ", buf);
        DB::writeText(queryToString(ASTLiteral(Field(comment))), buf);
    }

    if (codec)
    {
        writeChar('\t', buf);
        DB::writeText("CODEC(", buf);
        DB::writeText(codec->getCodecDesc(), buf);
        DB::writeText(")", buf);
    }

    writeChar('\n', buf);
}

void ColumnDescription::readText(ReadBuffer & buf)
{
    ParserColumnDeclaration column_parser(true);
    String column_line;
    readEscapedStringUntilEOL(column_line, buf);
    ASTPtr ast = parseQuery(column_parser, column_line, "column parser", 0);
    if (const ASTColumnDeclaration * col_ast = typeid_cast<const ASTColumnDeclaration *>(ast.get()))
    {
        name = col_ast->name;
        type = DataTypeFactory::instance().get(col_ast->type);

        if (col_ast->default_expression)
        {
            default_desc.kind = columnDefaultKindFromString(col_ast->default_specifier);
            default_desc.expression = std::move(col_ast->default_expression);
        }

        if (col_ast->comment)
            comment = typeid_cast<ASTLiteral &>(*col_ast->comment).value.get<String>();

        if (col_ast->codec)
            codec = CompressionCodecFactory::instance().get(col_ast->codec, type);
    }
    else
        throw Exception("Cannot parse column description", ErrorCodes::CANNOT_PARSE_TEXT);
}

ColumnsDescription::ColumnsDescription(NamesAndTypesList ordinary)
{
    for (auto & elem : ordinary)
        add(ColumnDescription(std::move(elem.name), std::move(elem.type)));
}

ColumnDefaults ColumnsDescription::getDefaults() const
{
    ColumnDefaults ret;
    for (const auto & column : columns)
    {
        if (column.default_desc.expression)
            ret.emplace(column.name, column.default_desc);
    }

    return ret;
}

void ColumnsDescription::flattenNested()
{
    /// TODO: implementation without the temporary NamesAndTypesList of all columns.
    auto all_columns = getAll();
    std::unordered_map<std::string, std::vector<std::string>> mapping;
    Nested::flattenWithMapping(all_columns, mapping);
    for (auto column_it = name_to_column.begin(); column_it != name_to_column.end(); )
    {
        auto mapping_it = mapping.find(column_it->second->name);
        if (mapping_it != mapping.end())
        {
            ColumnDescription column = std::move(*column_it->second);
            auto insert_it = columns.erase(column_it->second);
            for (const auto & nested_name : mapping_it->second)
            {
                auto nested_column = column;
                /// TODO: what to do with default expressions?
                nested_column.name = nested_name;
                insert_it = columns.insert(insert_it, std::move(nested_column));
                name_to_column.emplace(nested_name, insert_it);
                ++insert_it;
            }
        }
        else
            ++column_it;
    }
}


NamesAndTypesList ColumnsDescription::getOrdinary() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
    {
        if (col.default_desc.kind == ColumnDefaultKind::Default)
            ret.emplace_back(col.name, col.type);
    }
    return ret;
}

NamesAndTypesList ColumnsDescription::getMaterialized() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
    {
        if (col.default_desc.kind == ColumnDefaultKind::Materialized)
            ret.emplace_back(col.name, col.type);
    }
    return ret;
}

NamesAndTypesList ColumnsDescription::getAliases() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
    {
        if (col.default_desc.kind == ColumnDefaultKind::Alias)
            ret.emplace_back(col.name, col.type);
    }
    return ret;
}

NamesAndTypesList ColumnsDescription::getAllPhysical() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
    {
        if (col.default_desc.kind != ColumnDefaultKind::Alias)
            ret.emplace_back(col.name, col.type);
    }
    return ret;
}


NamesAndTypesList ColumnsDescription::getAll() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        ret.emplace_back(col.name, col.type);
    return ret;
}


Names ColumnsDescription::getNamesOfPhysical() const
{
    Names ret;
    for (const auto & col : columns)
    {
        if (col.default_desc.kind != ColumnDefaultKind::Alias)
            ret.emplace_back(col.name);
    }
    return ret;
}


NameAndTypePair ColumnsDescription::getPhysical(const String & column_name) const
{
    auto it = name_to_column.find(column_name);
    if (it == name_to_column.end() || it->second->default_desc.kind == ColumnDefaultKind::Alias)
        throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    return NameAndTypePair(it->second->name, it->second->type);
}


bool ColumnsDescription::hasPhysical(const String & column_name) const
{
    auto it = name_to_column.find(column_name);
    return it != name_to_column.end() && it->second->default_desc.kind != ColumnDefaultKind::Alias;
}


String ColumnsDescription::toString() const
{
    WriteBufferFromOwnString buf;

    writeCString("columns format version: 1\n", buf);
    DB::writeText(columns.size(), buf);
    writeCString(" columns:\n", buf);

    for (const ColumnDescription & column : columns)
        column.writeText(buf);

    return buf.str();
}


CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    const auto it = name_to_column.find(column_name);

    if (it == name_to_column.end() || !it->second->codec)
        return default_codec;

    return it->second->codec;
}


CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name) const
{
    return getCodecOrDefault(column_name, CompressionCodecFactory::instance().getDefaultCodec());
}

ColumnsDescription ColumnsDescription::parse(const String & str)
{
    ReadBufferFromString buf{str};

    assertString("columns format version: 1\n", buf);
    size_t count{};
    readText(count, buf);
    assertString(" columns:\n", buf);

    ColumnsDescription result;
    for (size_t i = 0; i < count; ++i)
    {
        ColumnDescription column;
        column.readText(buf);
        buf.ignore(1); /// ignore new line
        result.add(std::move(column));
    }

    assertEOF(buf);
    return result;
}

const ColumnsDescription * ColumnsDescription::loadFromContext(const Context & context, const String & db, const String & table)
{
    if (context.getSettingsRef().insert_sample_with_metadata)
    {
        if (context.isTableExist(db, table))
        {
            StoragePtr storage = context.getTable(db, table);
            return &storage->getColumns();
        }
    }

    return nullptr;
}

}
