using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public readonly record struct DeltaTime(DateTimeOffset Value)
{
    public static implicit operator DateTimeOffset(DeltaTime time) => time.Value;
    public static implicit operator DeltaTime(DateTimeOffset time) => new(time);
}

public sealed record class DeltaAdd
{
    public string Path { get; }

    public DeltaMap<string, string> PartitionValues { get; }

    public long Size { get; }

    public DeltaTime ModificationTime { get; }

    public bool DataChange { get; }

    public DeltaStats? Stats { get; set; } = null;

    public DeltaMap<string, string>? Tags { get; }

    public long? BaseRowId { get; }

    public long? DefaultRowCommitVersion { get; }

    public string? ClusteringProvider { get; }

    public DeltaAdd(string path, long size, DeltaTime modificationTime, bool dataChange, DeltaStats? stats = null, DeltaMap<string, string>? partitionValues = null, DeltaMap<string, string>? tags = null, long? baseRowId = null, long? defaultRowCommitVersion = null, string? clusteringProvider = null)
    {
        Path = path;
        PartitionValues = partitionValues ?? [];
        Size = size;
        ModificationTime = modificationTime;
        DataChange = dataChange;
        Stats = stats;
        Tags = tags;
        BaseRowId = baseRowId;
        DefaultRowCommitVersion = defaultRowCommitVersion;
        ClusteringProvider = clusteringProvider;
    }

}
