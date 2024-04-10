using System.Text;
using System.Text.Json;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DateTimeOffsetToMillisTests
{
    public static readonly JsonSerializerOptions options = new() { Converters = { new DateTimeOffsetToMillis() } };
    [Theory]
    [MemberData(nameof(ValidTestCases))]
    public void Read_WithValidTestCase_ShouldDeserialize(string json, DateTimeOffset expected)
    {

        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        var converter = new DateTimeOffsetToMillis();
        reader.Read();

        var actual = converter.Read(ref reader, typeof(DateTimeOffset), new JsonSerializerOptions());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(ValidTestCases))]
    public void Write_WithValidTestCase_ShouldSerialize(string expected, DateTimeOffset value)
    {
        var stream = new MemoryStream();
        var writer = new Utf8JsonWriter(stream);
        var converter = new DateTimeOffsetToMillis();

        converter.Write(writer, value, new JsonSerializerOptions());
        writer.Flush();

        var actual = Encoding.UTF8.GetString(stream.ToArray());

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(InvalidTestCases))]
    public void Read_WithInvalidTestCase_ShouldThrow(string json)
    {
        var converter = new DateTimeOffsetToMillis();

        Assert.Throws<JsonException>(() =>
        {
            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
            reader.Read();
            converter.Read(ref reader, typeof(DateTimeOffset), new JsonSerializerOptions());
        });
    }

    public static IEnumerable<object[]> ValidTestCases()
    {
        yield return new object[] { DateTimeOffset.MinValue.ToUnixTimeMilliseconds().ToString(), DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.MinValue.ToUnixTimeMilliseconds()) };
        yield return new object[] { DateTimeOffset.MaxValue.ToUnixTimeMilliseconds().ToString(), DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()) };
        yield return new object[] { DateTimeOffset.UnixEpoch.ToUnixTimeMilliseconds().ToString(), DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UnixEpoch.ToUnixTimeMilliseconds()) };
    }

    public static IEnumerable<object[]> InvalidTestCases()
    {
        yield return new object[] { "null" };
        yield return new object[] { "true" };
        yield return new object[] { "false" };
        yield return new object[] { "3.13" };
        yield return new object[] { "\"2012-06-11T01:02:03\"" };
        yield return new object[] { "[]" };
        yield return new object[] { "{}" };
    }
}