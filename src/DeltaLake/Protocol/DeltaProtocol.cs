using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaLake.Protocol;

public class DeltaProtocolJsonConverter : JsonConverter<DeltaProtocol>
{
    private static readonly JsonSerializerOptions internalOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public override DeltaProtocol? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return JsonSerializer.Deserialize<DeltaProtocol>(ref reader, internalOptions);
    }

    public override void Write(Utf8JsonWriter writer, DeltaProtocol value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("minReaderVersion", value.MinReaderVersion);
        writer.WriteNumber("minWriterVersion", value.MinWriterVersion);
        if (value.MinReaderVersion >= 3)
        {
            writer.WritePropertyName("readerFeatures");
            JsonSerializer.Serialize(writer, value.ReaderFeatures, internalOptions);
        }
        if (value.MinWriterVersion >= 7)
        {
            writer.WritePropertyName("writerFeatures");
            JsonSerializer.Serialize(writer, value.WriterFeatures, internalOptions);
        }
        writer.WriteEndObject();
    }
}

public sealed record class DeltaProtocol
{
    public int MinReaderVersion { get; } = 1;
    public int MinWriterVersion { get; } = 2;
    public DeltaArray<string> ReaderFeatures { get; } = [];
    public DeltaArray<string> WriterFeatures { get; } = [];

    public DeltaProtocol(int MinReaderVersion = 1, int MinWriterVersion = 2, DeltaArray<string>? ReaderFeatures = null, DeltaArray<string>? WriterFeatures = null)
    {
        this.MinReaderVersion = MinReaderVersion;
        this.MinWriterVersion = MinWriterVersion;
        if (ReaderFeatures is not null)
            this.ReaderFeatures = ReaderFeatures;
        if (WriterFeatures is not null)
            this.WriterFeatures = WriterFeatures;
    }

}
