namespace DeltaLake.Protocol;

public sealed record DeltaFormat
{
    public string Provider { get; }

    public DeltaMap<string, string> Options { get; }

    public DeltaFormat(string provider, DeltaMap<string, string> options)
    {
        Provider = provider;
        Options = options;
    }

    public static DeltaFormat Default { get; } = new("parquet", []);

}
