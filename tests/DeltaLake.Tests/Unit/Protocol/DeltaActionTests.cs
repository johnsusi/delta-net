namespace DeltaLake.Tests.Unit.Protocol;

using System.Text.Json;
using DeltaLake.Protocol;

public class DeltaActionTests
{

    [Fact]
    public void New_WithNoActions_ShouldThrow()
    {
        var exception = Assert.Throws<ArgumentException>(() => new DeltaAction());

        Assert.Equal("At least one action must be set", exception.Message);
    }

    [Fact]
    public void New_WithMultipleActions_ShouldThrow()
    {
        var metaData = new DeltaMetaData(Guid.NewGuid(), new DeltaSchema("struct", []), DeltaFormat.Default, [], []);
        var protocol = new DeltaProtocol();
        var exception = Assert.Throws<ArgumentException>(() => new DeltaAction(metaData: metaData, protocol: protocol));

        Assert.Equal("Only one action can be set", exception.Message);
    }

    [Theory]
    [MemberData(nameof(ValidTestCases))]
    public void FromJson_WithConformingJson_ShouldCreateAction(string json, DeltaAction expected)
    {
        var actual = DeltaAction.FromJson(json);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(ValidTestCases))]
    public void ToJson_WithValidAction_ShouldCreateConformingJson(string expected, DeltaAction action)
    {
        var actual = action.ToJson();

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("true")]
    [InlineData("3.14")]
    [InlineData("\"\"")]
    [InlineData("[]")]
    // [InlineData("{}")]
    public void FromJson_WithoutConformingJson_ShouldThrow(string json)
    {
        Assert.Throws<JsonException>(() => DeltaAction.FromJson(json));
    }

    // [Fact]
    // public void Serialize_ShouldCreateConformingJson()
    // {
    //     var expected = """{"metaData":{"id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa","format":{"provider":"parquet","options":{}},"schemaString":"...","partitionColumns":[],"configuration":{"appendOnly":"true"}}}""";
    //     var metaData = new DeltaMetaData()
    //     {
    //         Id = Guid.Parse("af23c9d7-fff1-4a5a-a2c8-55c59bd782aa"),
    //         SchemaString = "...",
    //         Configuration =
    //         {
    //             ["appendOnly"] = "true"
    //         }
    //     };
    //     var action = new DeltaAction(metaData);
    //     var actual = JsonSerializer.Serialize(action, DeltaAction.JsonSerializerOptions);

    //     Assert.Equal(expected, actual);
    // }

    public static IEnumerable<object[]> ValidTestCases()
    {
        yield return [
            """{"metaData":{"id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa","format":{"provider":"parquet","options":{}},"schemaString":"{\u0022type\u0022:\u0022struct\u0022,\u0022fields\u0022:[]}","partitionColumns":[],"configuration":{"appendOnly":"true"}}}""",
            new DeltaAction(metaData: new(Guid.Parse("af23c9d7-fff1-4a5a-a2c8-55c59bd782aa"), new DeltaSchema("struct", []), DeltaFormat.Default, [], [new ("appendOnly", "true")]))
        ];

        yield return [
            """{"add":{"path":"path","partitionValues":{},"size":1,"modificationTime":123456,"dataChange":true}}""",
            new DeltaAction(add: new("path", 1, DateTimeOffset.FromUnixTimeMilliseconds(123456), true))
        ];

        yield return [
            """{"remove":{"path":"path","dataChange":true}}""",
            new DeltaAction(remove: new("path", true))
        ];

        yield return [
            """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""",
            new DeltaAction(protocol: new(1, 2, [], []))
        ];

        yield return [
            """{"protocol":{"minReaderVersion":2,"minWriterVersion":7,"writerFeatures":["columnMapping","identityColumns"]}}""",
            new DeltaAction(protocol: new(2, 7, [], ["columnMapping", "identityColumns"]))
        ];

        yield return [
            """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping","identityColumns"]}}""",
            new DeltaAction(protocol: new(3, 7, ["columnMapping"], ["columnMapping","identityColumns"]))
        ];

    }
}

