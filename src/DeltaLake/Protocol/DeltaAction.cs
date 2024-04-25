using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public sealed record class DeltaAction
{
    public DeltaMetaData? MetaData { get; } = null;
    public DeltaAdd? Add { get; } = null;
    public DeltaRemove? Remove { get; } = null;
    public DeltaProtocol? Protocol { get; } = null;
    public JsonDocument? CommitInfo { get; } = null;

    public DeltaAction(DeltaMetaData? metaData = null, DeltaAdd? add = null, DeltaRemove? remove = null, DeltaProtocol? protocol = null, JsonDocument? commitInfo = null)
    {
        int count = 0;
        if (metaData is not null) count++;
        if (add is not null) count++;
        if (remove is not null) count++;
        if (protocol is not null) count++;
        if (commitInfo is not null) count++;

        if (count == 0) throw new ArgumentException("At least one action must be set");
        if (count != 1) throw new ArgumentException("Only one action can be set");

        MetaData = metaData;
        Add = add;
        Remove = remove;
        Protocol = protocol;
        CommitInfo = commitInfo;
    }

    public static JsonSerializerOptions JsonSerializerOptions { get; } = new()
    {
        Converters = {
            new DeltaStatsToString(),
            new DeltaTimeToLong(),
            new DeltaProtocolJsonConverter(),
        },
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static DeltaAction FromJson(string json)
    {
        return JsonSerializer.Deserialize<DeltaAction>(json, JsonSerializerOptions)
            ?? throw new JsonException("Expected DeltaAction");
    }
    public string ToJson()
    {
        return JsonSerializer.Serialize(this, JsonSerializerOptions);
    }

}
