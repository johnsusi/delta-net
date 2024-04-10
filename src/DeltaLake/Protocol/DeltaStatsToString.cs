using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public sealed class DeltaStatsToString : JsonConverter<DeltaStats?>
{

    private readonly JsonSerializerOptions _options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
    };

    public override DeltaStats? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("Expected string");
        var str = reader.GetString()
            ?? throw new JsonException("Expected string");
        return JsonSerializer.Deserialize<DeltaStats>(str, _options)
            ?? throw new JsonException("Expected DeltaStats");
    }

    public override void Write(Utf8JsonWriter writer, DeltaStats? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            return;
        }
        var str = JsonSerializer.Serialize(value, _options);
        writer.WriteStringValue(str);
    }
}
