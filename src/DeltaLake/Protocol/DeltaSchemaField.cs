using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public enum DeltaSchemaFieldTypes
{
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    String,
    Binary,
    Boolean,
    Timestamp,
    Date,
    Array,
    Map,
    Struct
}

public sealed record class DeltaSchemaField
{
    [JsonPropertyName("name")]
    [JsonRequired]
    public string Name { get; init; }

    [JsonPropertyName("type")]
    [JsonRequired]
    public string Type { get; init; }

    [JsonPropertyName("nullable")]
    [JsonRequired]
    public bool Nullable { get; init; }

    [JsonPropertyName("metadata")]
    [JsonRequired]
    public DeltaMap<string, string> Metadata { get; init; } = [];

    public static string[] ValidTypes =
    [
        "byte",
        "short",
        "integer",
        "long",
        "float",
        "double",
        "string",
        "binary",
        "boolean",
        "timestamp",
        "date",
        "array",
        "map",
        "struct"
    ];

    [JsonConstructor]
    public DeltaSchemaField(string name, string type, bool nullable, DeltaMap<string, string> metadata)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name is required", nameof(name));
        }

        if (string.IsNullOrEmpty(type) || !ValidTypes.Contains(type))
        {
            throw new ArgumentException($"Type is required {type}", nameof(type));
        }

        Name = name;
        Type = type;
        Nullable = nullable;
        Metadata = metadata;
    }

}
