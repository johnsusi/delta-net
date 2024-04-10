namespace DeltaLake.Protocol;

public sealed record class DeltaProtocol
{
    public int MinReaderVersion { get; } = 1;
    public int MinWriterVersion { get; } = 2;
    public DeltaArray<string> ReaderFeatures { get; } = [];
    public DeltaArray<string> WriterFeatures { get; } = [];

    public DeltaProtocol()
    {
    }

    public DeltaProtocol(int MinReaderVersion, int MinWriterVersion, DeltaArray<string> ReaderFeatures, DeltaArray<string> WriterFeatures)
    {
        this.MinReaderVersion = MinReaderVersion;
        this.MinWriterVersion = MinWriterVersion;
        this.ReaderFeatures = ReaderFeatures;
        this.WriterFeatures = WriterFeatures;
    }

}
