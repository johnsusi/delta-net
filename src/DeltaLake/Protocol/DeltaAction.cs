using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public sealed record class DeltaAction
{
    public DeltaMetaData? MetaData { get; }
    public DeltaAdd? Add { get; }
    public DeltaRemove? Remove { get; }
    public DeltaTransaction? Txn { get; }
    public DeltaProtocol? Protocol { get; }
    public JsonDocument? CommitInfo { get; }

    public DeltaAction(DeltaMetaData? metaData = null, DeltaAdd? add = null, DeltaRemove? remove = null, DeltaProtocol? protocol = null, JsonDocument? commitInfo = null,  DeltaTransaction? txn = null)
    {
        int count = 0;
        if (metaData is not null) count++;
        if (add is not null) count++;
        if (remove is not null) count++;
        if (protocol is not null) count++;
        if (commitInfo is not null) count++;
        if (txn is not null) count++;

        if (count == 0) throw new ArgumentException("At least one action must be set");
        if (count != 1) throw new ArgumentException("Only one action can be set");

        MetaData = metaData;
        Add = add;
        Remove = remove;
        Protocol = protocol;
        CommitInfo = commitInfo;
        Txn = txn;
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
