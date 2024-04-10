using System.Text;
using System.Text.Json;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaStatsToStringTests
{

    // [Fact]
    // public void Read_ValidJson_ReturnsDeltaStats()
    // {
    //     var expected = new DeltaStats(3,
    //         [new("foo", 0), new("bar", 1), new("baz", 2)],
    //         [new("foo", 3), new("bar", 4), new("baz", 5)],
    //         [new("foo", 0), new("bar", 0), new("baz", 0)]);
    //     // var json = "\"{\\\"numRecords\\\":0}\""u8;
    //     var json = "\"{\\\"numRecords\\\":3,\\\"minValues\\\":{\\\"foo\\\":0,\\\"bar\\\":1,\\\"baz\\\":2},\\\"maxValues\\\":{\\\"foo\\\":3,\\\"bar\\\":4,\\\"baz\\\":5},\\\"nullCount\\\":{\\\"foo\\\":0,\\\"bar\\\":0,\\\"baz\\\":0}}\""u8;
    //     var reader = new Utf8JsonReader(json);
    //     var converter = new DeltaStatsToString();
    //     reader.Read();

    //     var actual = converter.Read(ref reader, typeof(DeltaStats), new JsonSerializerOptions());

    //     Assert.Equal(expected, actual);
    // }

    [Theory]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("true")]
    [InlineData("3.13")]
    [InlineData("\"foo\"")]
    [InlineData("[]")]
    // [InlineData("{}")]
    public void Read_WrongType_Throws(string json)
    {
        var converter = new DeltaStatsToString();

        Assert.Throws<JsonException>(() =>
        {
            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
            reader.Read();
            converter.Read(ref reader, typeof(DeltaStats), new JsonSerializerOptions());
        });
    }

    // [Fact]
    // public void Write_ValidDeltaStats_WritesJson()
    // {
    //     var expected = "{\"numFiles\":123,\"numBytes\":456}";
    //     var value = new DeltaStats(123, 456);
    //     var stream = new MemoryStream();
    //     var writer = new Utf8JsonWriter(stream);
    //     var converter = new DeltaStatsToString();

    //     converter.Write(writer, value, new JsonSerializerOptions());
    //     writer.Flush();

    //     var actual = Encoding.UTF8.GetString(stream.ToArray());

    //     Assert.Equal(expected, actual);
    // }

}
