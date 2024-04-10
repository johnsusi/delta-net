namespace DeltaLake.Protocol;

public sealed record class DeltaRemove
{
    public string Path { get; }

    public long? DeletionTimestamp { get; }

    public bool DataChange { get; }

    public bool? ExtendedFileMetadata { get; }

    public DeltaMap<string, string>? PartitionValues { get; }

    public long? Size { get; }

    public DeltaStats? Stats { get; }

    public DeltaMap<string, string>? Tags { get; }

    public long? BaseRowId { get; }

    public long? DefaultRowCommitVersion { get; }

    public DeltaRemove(string path, bool dataChange, long? deletionTimestamp = null, bool? extendedFileMetadata = null, DeltaMap<string, string>? partitionValues = null, long? size = null, DeltaStats? stats = null, DeltaMap<string, string>? tags = null, long? baseRowId = null, long? defaultRowCommitVersion = null)
    {
        Path = path;
        DataChange = dataChange;
        DeletionTimestamp = deletionTimestamp;
        ExtendedFileMetadata = extendedFileMetadata;
        PartitionValues = partitionValues;
        Size = size;
        Stats = stats;
        Tags = tags;
        BaseRowId = baseRowId;
        DefaultRowCommitVersion = defaultRowCommitVersion;
    }

}
