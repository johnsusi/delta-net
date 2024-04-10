using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaSchemaTests
{

    [Theory]
    [MemberData(nameof(TestCases))]
    public void Serialize_ShouldReturnJson(string expected, DeltaSchema schema)
    {
        var actual = JsonSerializer.Serialize(schema, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(TestCases))]
    public void Deserialize_ShouldReturnSchema(string json, DeltaSchema expected)
    {
        var actual = JsonSerializer.Deserialize<DeltaSchema>(json, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void FromArrow_WithArrowSchema_ReturnsSchema()
    {
        var arrowSchema = new Schema.Builder()
            .Field(f => f.Name("int8").DataType(Int8Type.Default).Nullable(true).Build())
            .Field(f => f.Name("int16").DataType(Int16Type.Default).Nullable(true).Build())
            .Field(f => f.Name("int32").DataType(Int32Type.Default).Nullable(true).Build())
            .Field(f => f.Name("int64").DataType(Int64Type.Default).Nullable(true).Build())
            .Field(f => f.Name("uint8").DataType(UInt8Type.Default).Nullable(true).Build())
            .Field(f => f.Name("uint16").DataType(UInt16Type.Default).Nullable(true).Build())
            .Field(f => f.Name("uint32").DataType(UInt32Type.Default).Nullable(true).Build())
            .Field(f => f.Name("uint64").DataType(UInt64Type.Default).Nullable(true).Build())
            .Field(f => f.Name("float").DataType(FloatType.Default).Nullable(true).Build())
            .Field(f => f.Name("double").DataType(DoubleType.Default).Nullable(true).Build())
            .Field(f => f.Name("string").DataType(StringType.Default).Nullable(true).Build())
            .Field(f => f.Name("binary").DataType(BinaryType.Default).Nullable(true).Build())
            .Field(f => f.Name("bool").DataType(BooleanType.Default).Nullable(true).Build())
            .Field(f => f.Name("timestamp").DataType(TimestampType.Default).Nullable(true).Build())
            .Field(f => f.Name("date32").DataType(Date32Type.Default).Nullable(true).Build())
            .Field(f => f.Name("date64").DataType(Date64Type.Default).Nullable(true).Build())
            // .Field(f => f.Name("array").DataType(new ListType(Int32Type.Default)).Nullable(true).Build())
            // .Field(f => f.Name("map").DataType(new MapType(Int32Type.Default, StringType.Default)).Nullable(true).Build())
            // .Field(f => f.Name("struct").DataType(new StructType(new Field[] { })).Nullable(true).Build())

            .Build();
        var deltaSchema = DeltaSchema.FromArrow(arrowSchema);

        Assert.NotNull(deltaSchema);
    }

    [Fact]
    public void New_WithoutStructType_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => new DeltaSchema("not struct", []));
    }

    [Fact]
    public void New_WithMissingName_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => new DeltaSchemaField(string.Empty, "integer", true, []));
    }

    [Fact]
    public void New_WithMissingType_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => new DeltaSchemaField("name", string.Empty, true, []));
    }

    [Fact]
    public void New_WithInvalidType_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() => new DeltaSchemaField("name", "not a valid type", true, []));
    }


    public static IEnumerable<object[]> TestCases()
    {
        yield return [
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"intCol\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
            new DeltaSchema("struct", [
                new DeltaSchemaField("intCol", "integer", true, [])
            ])
        ];
    }

}
