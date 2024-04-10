using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public sealed class DateTimeOffsetToMillis : JsonConverter<DateTimeOffset>
{
    public override bool HandleNull => true;
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var value))
            return DateTimeOffset.FromUnixTimeMilliseconds(value);
        throw new JsonException("Expected integer");
    }

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
    {
        writer.WriteNumberValue(value.ToUnixTimeMilliseconds());
    }
}

public sealed class DeltaTimeToLong : JsonConverter<DeltaTime>
{
    public override bool HandleNull => true;
    public override DeltaTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var value))
            return DateTimeOffset.FromUnixTimeMilliseconds(value);
        throw new JsonException("Expected integer");
    }

    public override void Write(Utf8JsonWriter writer, DeltaTime value, JsonSerializerOptions options)
    {
        writer.WriteNumberValue(value.Value.ToUnixTimeMilliseconds());
    }
}