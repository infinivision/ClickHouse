#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <memory>
#include <utility>
#include <chrono>

namespace DB
{

template <typename Point>
class FunctionPolygonsIntersection : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsIntersection() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsIntersection>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return DataTypeCustomMultiPolygonSerialization::nestedDataType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        checkColumnTypeOrThrow<Point, MultiPolygon>(arguments[0]);
        auto first_parser = MultiPolygonFromColumnParser<Point>(std::move(arguments[0].column->convertToFullColumnIfConst()));
        

        checkColumnTypeOrThrow<Point, MultiPolygon>(arguments[1]);
        auto second_parser = MultiPolygonFromColumnParser<Point>(std::move(arguments[1].column->convertToFullColumnIfConst()));        

        MultiPolygonSerializer<Point> serializer;
        MultiPolygon<Point> intersection{};

        auto first = first_parser.parse(0, 0);
        auto second = second_parser.parse(0, 0);

        /// We are not interested in some pitfalls in third-party libraries
        /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            MultiPolygon<Point> first_container = first[i];
            MultiPolygon<Point> second_container = second[i];

            /// Orient the polygons correctly.
            boost::geometry::correct(first_container);
            boost::geometry::correct(second_container);

            /// Main work here.
            boost::geometry::intersection(first_container[0], second_container[0], intersection);

            serializer.add(intersection);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


template <>
const char * FunctionPolygonsIntersection<CartesianPoint>::name = "polygonsIntersectionCartesian";

template <>
const char * FunctionPolygonsIntersection<GeographicPoint>::name = "polygonsIntersectionGeographic";


void registerFunctionPolygonsIntersection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsIntersection<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonsIntersection<GeographicPoint>>();
}

}
