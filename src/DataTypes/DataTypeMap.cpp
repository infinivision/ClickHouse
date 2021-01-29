#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>

#include <ext/map.h>
#include <ext/enumerate.h>
#include <tuple>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int ILLEGAL_COLUMN;
}

DataTypeMap::DataTypeMap(const DataTypePtr & value_type_)
:key_type(std::make_shared<DataTypeString>()),
value_type(makeNullable(value_type_))
{
}

std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << value_type->getName() << ")";

    return s.str();
}

DataTypePtr DataTypeMap::tryGetSubcolumnType(const String & subcolumn_name) const
{
    size_t loc = subcolumn_name.find_first_of(".");
    String first_name, last_name;
    if (loc == std::string::npos)
    {
        first_name = subcolumn_name;
    }
    else{
        first_name = subcolumn_name.substr(0, loc);
        last_name = subcolumn_name.substr(loc+1);
    }
    if (last_name.empty())
        return value_type;
    return value_type->tryGetSubcolumnType(last_name);
}

ColumnPtr DataTypeMap::getSubcolumn(const String & subcolumn_name, const IColumn & column) const
{
    const auto & colMap = assert_cast<const ColumnMap &>(column);
    size_t loc = subcolumn_name.find_first_of(".");
    String first_name, last_name;
    if (loc == std::string::npos)
    {
        first_name = subcolumn_name;
    }
    else{
        first_name = subcolumn_name.substr(0, loc);
        last_name = subcolumn_name.substr(loc+1);
    }
    auto it = colMap.subColumns.find(first_name);
    if (it == colMap.subColumns.end())
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in type {}", subcolumn_name, getName());
    }
    if (last_name.empty())
        return it->second;
    return value_type->getSubcolumn(last_name, *(it->second));
}

template <typename KeyWriter, typename ValWriter>
void DataTypeMap::serializeTextImpl(const IColumn & column, WriteBuffer & ostr, KeyWriter && keyWriter, ValWriter valWriter) const
{
    const auto & colMap = assert_cast<const ColumnMap &>(column);
    bool first = true;
    writeChar('{', ostr);
    for (const auto & elem : colMap.subColumns)
    {
        if (first)
            writeChar(',', ostr);
        first = false;
        keyWriter(elem.first);
        writeChar(':', ostr);
        valWriter(*(elem.second));
    }
    writeChar('}', ostr);
}

template <typename KeyReader, typename ValReader>
void DataTypeMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, KeyReader && keyReader, ValReader && valReader) const
{
    ColumnMap & colMap = assert_cast<ColumnMap &>(column);
    auto & subColumns = colMap.subColumns;
    size_t orig_size = column.size();

    assertChar('{', istr);
    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw Exception("Cannot read Map from text", ErrorCodes::CANNOT_READ_MAP_FROM_TEXT);
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            String keyStr;
            keyReader(keyStr);
            skipWhitespaceIfAny(istr);
            assertChar(':', istr);
            skipWhitespaceIfAny(istr);
            auto it = subColumns.find(keyStr);
            if (it == subColumns.end())
            {
                MutableColumnPtr mcp = value_type->createColumn();
                mcp->insertManyDefaults(orig_size);
                valReader(*mcp);
                subColumns[keyStr] = std::move(mcp);
            }
            else
            {
                valReader(*(it->second->assumeMutable()));
            }
            skipWhitespaceIfAny(istr);
        }
        assertChar('}', istr);

        for (auto & elem : colMap.subColumns)
        {
            if (elem.second->size()==orig_size)
            {
                elem.second->assumeMutable()->insertDefault();
            }
        }
    }
    catch (...)
    {
        throw;
    }
}

void DataTypeMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, ostr,
        [&](const String & keyStr)
        {
            writeQuotedString(keyStr, ostr);
        },
        [&](const IColumn & subcolumn)
        {
            value_type->serializeAsTextQuoted(subcolumn, row_num, ostr, settings);
        });
}

void DataTypeMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](String & keyStr)
        {
            readQuotedString(keyStr, istr);
        },
        [&](IColumn & subcolumn)
        {
            value_type->deserializeAsTextQuoted(subcolumn, istr, settings);
        });
}


void DataTypeMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, ostr,
        [&](const String & keyStr)
        {
            writeJSONString(keyStr, ostr, settings);
        },
        [&](const IColumn & subcolumn)
        {
            value_type->serializeAsTextJSON(subcolumn, row_num, ostr, settings);
        });
}

void DataTypeMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](String & keyStr)
        {
            readJSONString(keyStr, istr);
        },
        [&](IColumn & subcolumn)
        {
            value_type->deserializeAsTextJSON(subcolumn, istr, settings);
        });
}

void DataTypeMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void DataTypeMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings);
}

void DataTypeMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & colMap = assert_cast<const ColumnMap &>(column);
    writeCString("<map>", ostr);
    for (const auto & elem : colMap.subColumns)
    {
        writeCString("<elem>", ostr);
        writeCString("<key>", ostr);
        writeString(elem.first, ostr);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value_type->serializeAsTextXML(*(elem.second), row_num, ostr, settings);
        writeCString("</value>", ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</map>", ostr);
}

void DataTypeMap::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & map = get<const Map &>(field);
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        key_type->serializeBinary(elem.first, ostr);
        value_type->serializeBinary(elem.second, ostr);
    }
}

void DataTypeMap::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    auto & map = get<Map &>(field);
    size_t size;
    readVarUInt(size, istr);
    for (int i=0; i<int(size); i++)
    {
        Field key, value;
        key_type->deserializeBinary(key, istr);
        value_type->deserializeBinary(value, istr);
        map[get<String &>(key)] = std::move(value);
    }
}

void DataTypeMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    auto & colMap = assert_cast<const ColumnMap &>(column);
    auto & subColumns = colMap.subColumns;
    writeVarUInt(subColumns.size(), ostr);
    for (const auto & elem : subColumns)
    {
        key_type->serializeBinary(elem.first, ostr);
        value_type->serializeBinary(*(elem.second), row_num, ostr);
    }
}

void DataTypeMap::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnMap & colMap = assert_cast<ColumnMap &>(column);
    auto & subColumns = colMap.subColumns;
    size_t orig_size = column.size();
    Int32 size;
    readVarInt(size, istr);
    for (size_t i = 0; i < size_t(size); i++)
    {
        Field key, value;
        key_type->deserializeBinary(key, istr);
        String & keyStr = get<String &>(key);
        const auto & it = subColumns.find(keyStr);
        if (it == subColumns.end())
        {
            MutableColumnPtr mcp = value_type->createColumn();
            mcp->insertManyDefaults(orig_size);
            value_type->deserializeBinary(*mcp, istr);
            subColumns[keyStr] = std::move(mcp);
        }
        else
        {
            value_type->deserializeBinary(*(it->second->assumeMutable()), istr);
        }
    }
    for (auto & elem : subColumns)
    {
        if (elem.second->size()==orig_size)
        {
            elem.second->assumeMutable()->insertDefault();
        }
    }
}


void DataTypeMap::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    const auto & colMap = assert_cast<const ColumnMap &>(column);
    auto & subColumns = colMap.subColumns;
    protobuf.writeNumber(UInt32(subColumns.size()));
    for (auto & elem : subColumns)
    {
        protobuf.writeString(elem.first);
        value_type->serializeProtobuf(*(elem.second), row_num, protobuf, value_index);
    }
    value_index = true;
}

void DataTypeMap::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    auto & colMap = assert_cast<ColumnMap &>(column);
    auto & subColumns = colMap.subColumns;
    UInt32 numSubCols;
    if (!protobuf.readNumber(numSubCols))
        return;
    PaddedPODArray<UInt8> str;
    size_t orig_size = column.size();
    for (size_t i=0; i<size_t(numSubCols); i++)
    {
        if (!protobuf.readStringInto(str))
            return;
        String keyStr(reinterpret_cast<const char *>(str.data()), str.size());
        allow_add_row = true;
        const auto & it = subColumns.find(keyStr);
        if (it == subColumns.end())
        {
            MutableColumnPtr mcp = value_type->createColumn();
            mcp->insertManyDefaults(orig_size);
            value_type->deserializeProtobuf(*mcp, protobuf, allow_add_row, row_added);
            subColumns[keyStr] = std::move(mcp);
        }
        else
        {
            value_type->deserializeProtobuf(*(it->second->assumeMutable()), protobuf, allow_add_row, row_added);
        }
    }
    for (auto & elem : subColumns)
    {
        if (elem.second->size()==orig_size)
        {
            elem.second->assumeMutable()->insertDefault();
        }
    }
    allow_add_row = false;
    row_added = true;
}

void DataTypeMap::enumerateStreamsImpl(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::MapKeys);
    callback(path, *this);
    path.back() = Substream::MapValues;
    path.back().map_key = "FIXME"; //FIXME!!!
    value_type->enumerateStreams(callback, path);
    path.pop_back();
}

struct SerializeBinaryBulkStateMap : public IDataType::SerializeBinaryBulkState
{
    std::map<String, IDataType::SerializeBinaryBulkStatePtr> states;
};
struct DeserializeBinaryBulkStateMap : public IDataType::DeserializeBinaryBulkState
{
    std::map<String, IDataType::DeserializeBinaryBulkStatePtr> states;
};

static SerializeBinaryBulkStateMap * checkAndGetMapSerializeState(IDataType::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeMap.", ErrorCodes::LOGICAL_ERROR);

    auto * map_state = typeid_cast<SerializeBinaryBulkStateMap *>(state.get());
    if (!map_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeMap. Expected: "
                        + demangle(typeid(SerializeBinaryBulkStateMap).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return map_state;
}

void DataTypeMap::serializeBinaryBulkStatePrefixImpl(
    SerializeBinaryBulkSettings & /*settings*/,
    SerializeBinaryBulkStatePtr & /*state*/) const
{
    return;
}

void DataTypeMap::serializeBinaryBulkStateSuffixImpl(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetMapSerializeState(state);

    settings.path.push_back(Substream::MapValues);
    for (auto & elem : map_state->states)
    {
        settings.path.back().map_key = elem.first;
        value_type->serializeBinaryBulkStateSuffix(settings, elem.second);
    }
    settings.path.pop_back();
}

void DataTypeMap::deserializeBinaryBulkStatePrefixImpl(
    DeserializeBinaryBulkSettings & /*settings*/,
    DeserializeBinaryBulkStatePtr & /*state*/) const
{
    return;
}


void DataTypeMap::serializeBinaryBulkWithMultipleStreamsImpl(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnMap & colMap = assert_cast<const ColumnMap &>(column);
    auto map_state = std::make_shared<SerializeBinaryBulkStateMap>();
    /// First serialize map keys.
    settings.path.push_back(Substream::MapKeys);
    if (auto * stream = settings.getter(settings.path))
    {
        key_type->serializeBinaryBulk(*(colMap.keysColumn), *stream, offset, limit);
    }

    /// Then serialize contents of each subcolumn.
    settings.path.push_back(Substream::MapValues);
    for (const auto & elem : colMap.subColumns)
    {
        settings.path.back().map_key = elem.first;
        // Create a state for each key.
        SerializeBinaryBulkStatePtr sub_state = std::make_shared<SerializeBinaryBulkState>();
        value_type->serializeBinaryBulkStatePrefix(settings, sub_state);
        value_type->serializeBinaryBulkWithMultipleStreams(*(elem.second), offset, limit, settings, sub_state);
        map_state->states[elem.first] = sub_state;
    }
    state = std::move(map_state);
    settings.path.pop_back();
}

void DataTypeMap::deserializeBinaryBulkWithMultipleStreamsImpl(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnMap & colMap = assert_cast<ColumnMap &>(column);
    size_t orig_size = colMap.size();
    size_t delta = 0;
    MutableColumnPtr keysColumn = colMap.keysColumn->assumeMutable();
    auto & subColumns = colMap.subColumns;
    auto map_state = std::make_shared<DeserializeBinaryBulkStateMap>();

    /// First deserialize map keys.
    MutableColumnPtr src_keys_column = key_type->createColumn();
    settings.path.push_back(Substream::MapKeys);
    if (auto * stream = settings.getter(settings.path))
    {
        key_type->deserializeBinaryBulk(*src_keys_column, *stream, 0, 0);
    }

    /// Then deserialize contents of each subcolumn.
    settings.path.push_back(Substream::MapValues);
    for (size_t i = 0; i < src_keys_column->size(); ++i)
    {
        const Field & fld = (*src_keys_column)[i];
        const String & key = safeGet<String>(fld);
        settings.path.back().map_key = key;
        // Create a state for each key.
        DeserializeBinaryBulkStatePtr sub_state = std::make_shared<DeserializeBinaryBulkState>();
        const auto & it = subColumns.find(key);
        ColumnPtr cp;
        if (it == subColumns.end())
        {
            keysColumn->insert(key);
            MutableColumnPtr mcp = value_type->createColumn();
            mcp->insertManyDefaults(orig_size);
            cp = std::move(mcp);
            value_type->deserializeBinaryBulkWithMultipleStreams(cp, limit, settings, sub_state, cache);
            delta = cp->size() - orig_size;
            subColumns[key] = cp;
        }
        else if (it->second->size() == orig_size)
        {
            cp = it->second;
            value_type->deserializeBinaryBulkWithMultipleStreams(cp, limit, settings, sub_state, cache);
            delta = cp->size() - orig_size;
        }
        map_state->states[key] = sub_state;
    }
    for (const auto & elem : subColumns)
    {
        if (elem.second->size() == orig_size)
        {
            elem.second->assumeMutable()->insertManyDefaults(delta);
        }
    }
    state = std::move(map_state);
    settings.path.pop_back();
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(value_type)->assumeMutable();
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;
    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return value_type->equals(*rhs_map.value_type);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Map data type family must have one arguments: value type", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    const ASTPtr & child = arguments->children[0];
    const auto & value_type = DataTypeFactory::instance().get(child);
    return std::make_shared<DataTypeMap>(value_type);
}

void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
