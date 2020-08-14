#include <algorithm>
#include <vector>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include "arrayScalarProduct.h"

/** The function calculate inner-product of two arrays.
  * arrayIP(arr1, arr2) is equivalent to arraySum(arrayMap((x, y) -> (x*y), arr1, arr2). 
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

class FunctionArrayIP : public IFunction
{
public:
    static constexpr auto name = "arrayIP";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayIP>(); }

private:
    using ResultType = Float64;
    using ResultColumnType = ColumnVector<ResultType>;

    template <typename T>
    void executeImplData(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        ColumnPtr col1 = block.getByPosition(arguments[0]).column->convertToFullColumnIfConst();
        ColumnPtr col2 = block.getByPosition(arguments[1]).column->convertToFullColumnIfConst();
        if (!col1 || !col2)
            throw Exception("Array arguments for function " + getName() + " are null pointers1", ErrorCodes::BAD_ARGUMENTS);

        const ColumnArray * col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        const ColumnArray * col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (!col_array1 || !col_array2)
            throw Exception("Array arguments for function " + getName() + " are null pointers2", ErrorCodes::BAD_ARGUMENTS);

        if (!col_array1->hasEqualOffsets(*col_array2))
            throw Exception("Array arguments for function " + getName() + " must have equal sizes", ErrorCodes::BAD_ARGUMENTS);

        const ColumnVector<T> * col_nested1 = checkAndGetColumn<ColumnVector<T>>(col_array1->getData());
        const ColumnVector<T> * col_nested2 = checkAndGetColumn<ColumnVector<T>>(col_array2->getData());
        if (!col_nested1 || !col_nested2)
            throw Exception("Array arguments for function " + getName() + " are null pointers3", ErrorCodes::BAD_ARGUMENTS);

        auto & data1 = col_nested1->getData();
        auto & data2 = col_nested2->getData();
        auto & offsets = col_array1->getOffsets();
        size_t size = offsets.size();
        auto col_res = ResultColumnType::create();
        PaddedPODArray<ResultType> & res_array = col_res->getData();
        res_array.resize_fill(size, 0);
        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            for (size_t j = 0; j < array_size; ++j)
            {
                res_array[i] += ResultType(data1[current_offset + j] * data2[current_offset + j]);
            }
            current_offset = offsets[i];
        }
        block.getByPosition(result).column = std::move(col_res);
    }

public:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        // Basic type check
        std::vector<WhichDataType> whichs(2);
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception("All arguments for function " + getName() + " must be an array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto & nested_type = array_type->getNestedType();
            WhichDataType which(nested_type);
            if (!which.isFloat())
                throw Exception("All arguments for function " + getName() + " must be an array of float", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            whichs[i] = which;
        }
        if (whichs[0].isFloat32() != whichs[1].isFloat32())
            throw Exception("All arguments for function " + getName() + " must be an array of same float type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(block.getByPosition(arguments[0]).type.get());
        auto & nested_type = array_type->getNestedType();
        WhichDataType which(nested_type);
        if (which.isFloat32())
            executeImplData<Float32>(block, arguments, result);
        else
            executeImplData<Float64>(block, arguments, result);
    }
};


void registerFunctionArrayIP(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayIP>();
}
}
