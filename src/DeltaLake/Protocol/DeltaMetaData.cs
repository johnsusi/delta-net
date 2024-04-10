using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public class DeltaSchemaToString : JsonConverter<DeltaSchema>
{
    public override DeltaSchema? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            var value = reader.GetString();
            if (string.IsNullOrEmpty(value))
                return null;
            return JsonSerializer.Deserialize<DeltaSchema>(value, options);
        }
        throw new JsonException("Expected string");
    }

    public override void Write(Utf8JsonWriter writer, DeltaSchema value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(JsonSerializer.Serialize(value, options));
    }
}

public sealed record class DeltaMetaData
{
    public Guid Id { get; }

    public string? Name { get; }

    public string? Description { get; }

    public DeltaFormat Format { get; }

    [JsonConverter(typeof(DeltaSchemaToString))]
    [JsonPropertyName("schemaString")]
    public DeltaSchema Schema { get; }

    public DeltaArray<string> PartitionColumns { get; }

    [JsonConverter(typeof(DateTimeOffsetToMillis))]
    public DateTimeOffset? CreatedTime { get; }

    public DeltaMap<string, string> Configuration { get; }

    public DeltaMetaData(
        Guid id,
        DeltaSchema schema,
        DeltaFormat format,
        DeltaArray<string> partitionColumns,
        DeltaMap<string, string> configuration,
        string? name = null,
        string? description = null,
        DateTimeOffset? createdTime = null)
    {
        Id = id;
        Schema = schema;
        Format = format;
        PartitionColumns = partitionColumns;
        Configuration = configuration;
        Name = name;
        Description = description;
        CreatedTime = createdTime;
    }
}
