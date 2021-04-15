#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

class FunctionCompareVersion : public IFunction
{
public:
    static constexpr auto name = "compareVersion";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCompareVersion>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !isString(arguments[2]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt8>();
    }

    void vector(ReadBuffer & buf, PaddedPODArray<Int64> & comps, char separator) const
    {
        bool isNumber = true;
        Int64 res = 0;
        while (!buf.eof())
        {
            switch (*buf.position())
            {
                case '0':
                    [[fallthrough]];
                case '1':
                    [[fallthrough]];
                case '2':
                    [[fallthrough]];
                case '3':
                    [[fallthrough]];
                case '4':
                    [[fallthrough]];
                case '5':
                    [[fallthrough]];
                case '6':
                    [[fallthrough]];
                case '7':
                    [[fallthrough]];
                case '8':
                    [[fallthrough]];
                case '9':
                    res *= 10;
                    res += *buf.position() - '0';
                    break;
                default:
                    if (*buf.position() == separator)
                    {
                        if (isNumber)
                        {
                            comps.emplace_back(res);
                        }
                        else
                        {
                            comps.emplace_back(0);
                        }
                        res = 0;
                        isNumber = true;
                    }
                    else
                    {
                        isNumber = false;
                    }
            }
            ++buf.position();
        }
        if (res > 0 and isNumber)
        {
            comps.emplace_back(res);
        }
    }

    Int8 compare(const ColumnInt64 & leftComps, const ColumnInt64 & rightComps) const
    {
        Int8 res = 0;
        size_t length = std::max(leftComps.size(), rightComps.size());
        for (size_t i = 0; i < length; i++)
        {
            Int64 va = i < leftComps.size() ? leftComps.getElement(i) : 0;
            Int64 vb = i < rightComps.size() ? rightComps.getElement(i) : 0;
            Int64 compare = va - vb;
            if (compare != 0)
            {
                compare > 0 ? res = 1 : res = -1;
                return res;
            }
        }
        return res;
    }
    
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        size_t number_of_arguments = arguments.size();
        char separator;

        if (number_of_arguments == 3)
        {
            const ColumnConst * col_sep_const = checkAndGetColumnConstStringOrFixedString(block.getByPosition(arguments[2]).column.get());

            if (!col_sep_const)
                throw Exception(
                    "Illegal column " + block.getByPosition(arguments[2]).column->getName() + " of third argument of function " + getName()
                        + ". Must be constant string.",
                    ErrorCodes::ILLEGAL_COLUMN);

            String sep_str = col_sep_const->getValue<String>();
            if (sep_str.size() != 1)
                throw Exception("Illegal separator for function " + getName() + ". Must be exactly one byte.", ErrorCodes::BAD_ARGUMENTS);
                      
            separator = sep_str[0];
        }
        else
        {
            separator = '.';
        }
        const ColumnConst * col_right_const = checkAndGetColumnConstStringOrFixedString(block.getByPosition(arguments[1]).column.get());
        if (!col_right_const)
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + " of second argument of function " + getName()
                    + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);
        }
        ReadBufferFromMemory col_right_buffer(col_right_const->getDataAt(0).data, col_right_const->getDataAt(0).size);
        auto col_right_comps = ColumnInt64::create();
        this->vector(col_right_buffer, col_right_comps->getData(), separator);

        const ColumnString * col_left = checkAndGetColumn<ColumnString>(block.getByPosition(arguments[0]).column.get());
        const ColumnConst * col_left_const = checkAndGetColumnConstStringOrFixedString(block.getByPosition(arguments[0]).column.get());

        if (col_left)
        {
            const ColumnString::Chars & left_chars = col_left->getChars();
            const ColumnString::Offsets & left_offsets = col_left->getOffsets();
            size_t size = left_offsets.size();

            auto c_res = ColumnInt8::create();
            ColumnInt8::Container & vec_res = c_res->getData();
            vec_res.reserve(size);

            ColumnString::Offset prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory col_left_buffer(left_chars.data() + prev_offset, left_offsets[i] - prev_offset - 1);
                auto col_left_comps = ColumnInt64::create();
                this->vector(col_left_buffer, col_left_comps->getData(), separator);
                Int8 v = compare(*col_left_comps, *col_right_comps);
                c_res->insertValue(v);
                prev_offset = left_offsets[i];
            }
            block.getByPosition(result).column = std::move(c_res);
        }
        else if (col_left_const)
        {
            ReadBufferFromMemory col_left_buffer(col_left_const->getDataAt(0).data, col_left_const->getDataAt(0).size);
            auto col_left_comps = ColumnInt64::create();
            this->vector(col_left_buffer, col_left_comps->getData(), separator);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(input_rows_count, compare(*col_left_comps, *col_right_comps));
         }
        else
            throw Exception(
                "Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " of arguments of function " + getName()
                    + ". Must be string or constant string",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionCompareVersion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCompareVersion>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("cmpVer", FunctionCompareVersion::name, FunctionFactory::CaseInsensitive);
}

}