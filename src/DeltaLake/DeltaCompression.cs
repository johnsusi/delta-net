using ParquetSharp;

namespace DeltaLake;

public enum DeltaCompression
{
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Zstd,
    Lz4,
    Lz4Frame,
    Lzo,
    Bz2,
    Lz4Hadoop,
}

public static class DeltaCompressionExtensions
{

    public static Compression ToParquetSharpCompression(this DeltaCompression compression) =>
        compression switch
        {
            DeltaCompression.Uncompressed => Compression.Uncompressed,
            DeltaCompression.Snappy => Compression.Snappy,
            DeltaCompression.Gzip => Compression.Gzip,
            DeltaCompression.Brotli => Compression.Brotli,
            DeltaCompression.Zstd => Compression.Zstd,
            DeltaCompression.Lz4 => Compression.Lz4,
            DeltaCompression.Lz4Frame => Compression.Lz4Frame,
            DeltaCompression.Lzo => Compression.Lzo,
            DeltaCompression.Bz2 => Compression.Bz2,
            DeltaCompression.Lz4Hadoop => Compression.Lz4Hadoop,
            _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, null)
        };

}