using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using DeltaLake.Protocol;
using Microsoft.VisualBasic;
using ParquetSharp;
using ParquetSharp.Arrow;

namespace DeltaLake;

public class DeltaTable
{

    private readonly string _logPath = "_delta_log";
    public int Version { get; private set; } = -1;

    public DeltaMetaData MetaData { get; private set; }
    public Guid Id => MetaData.Id;
    public string? Name => MetaData.Name;
    public string? Description => MetaData.Description;
    public DeltaSchema Schema => MetaData.Schema;

    public IDeltaFileSystem FileSystem { get; }

    public DeltaArray<string> Files { get; private set; } = [];

    public DeltaArray<DeltaAction> Log { get; private set; } = [];

    public DeltaTable(
        IDeltaFileSystem fileSystem,
        DeltaArray<DeltaAction> log,
        int version
    )
    {
        FileSystem = fileSystem;
        Log = log;
        Version = version;
        foreach (var action in Log)
        {
            switch (action)
            {
                case { MetaData: not null }:
                    MetaData = action.MetaData;
                    break;
                case { Add: not null }:
                    Files.Add(action.Add.Path);
                    break;
                case { Remove: not null }:
                    Files.Remove(action.Remove.Path);
                    break;
                case { Protocol: not null }:
                    break;
                case { CommitInfo: not null }:
                    break;
                default:
                    throw new Exception($"Invalid action: {action}");

            }
        }
        if (MetaData is null)
        {
            throw new Exception("Meta data not found");
        }
    }

    internal DeltaTable(DeltaTable other)
    {
        FileSystem = other.FileSystem;
        Version = other.Version;
        MetaData = other.MetaData;
        Files = other.Files;
        Log = other.Log;
    }

    internal DeltaTable(IDeltaFileSystem fileSystem)
    {
        FileSystem = fileSystem;

        while (true)
        {
            var entryPath = Path.Combine(_logPath, $"{Version + 1:D20}.json");
            if (!FileSystem.FileExists(entryPath)) break;

            foreach (var line in FileSystem.ReadAllLines(entryPath))
            {
                var action = DeltaAction.FromJson(line)
                    ?? throw new Exception("Invalid action");
                Log.Add(action);
                switch (action)
                {
                    case { MetaData: not null }:
                        MetaData = action.MetaData;
                        break;
                    case { Add: not null }:
                        Files.Add(action.Add.Path);
                        break;
                    case { Remove: not null }:
                        Files.Remove(action.Remove.Path);
                        break;
                    case { Protocol: not null }:
                        break;
                    case { CommitInfo: not null }:
                        break;
                    default:
                        throw new Exception($"Invalid action: {action}");
                }
            }
            ++Version;
        }

        if (MetaData is null)
        {
            throw new Exception("Meta data not found");
        }

    }

    public static void WriteEntry(IDeltaFileSystem fileSystem, int version, IEnumerable<DeltaAction> actions)
    {
        var tempFile = fileSystem.CreateTempFile();
        var entryPath = Path.Combine("_delta_log", $"{version:D20}.json");
        var lines = actions.Select(action => action.ToJson());
        fileSystem.WriteFile(tempFile, lines);
        if (!fileSystem.MoveFile(tempFile, entryPath))
        {
            throw new Exception("Failed to write entries");
        }
    }

    public async IAsyncEnumerable<RecordBatch> GetRecordBatches(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        foreach (var path in Files)
        {
            using var stream = FileSystem.OpenRead(path);
            using var fileReader = new FileReader(stream);
            using var batchReader = fileReader.GetRecordBatchReader();
            while (true)
            {
                var batch = await batchReader.ReadNextRecordBatchAsync(cancellationToken);
                if (batch == null)
                    break;
                yield return batch;
            }
        }
    }

    public bool HasColumn(string column) => Schema.Fields.Where(f => f.Name == column).Any();

    private object? ConvertToSchemaType(object? value, string column)
    {
        var type = Schema.Fields.Where(f => f.Name == column).First().Type;
        return (type, value) switch
        {
            ("byte", null) => (sbyte?)null,
            ("byte", JsonElement el) => el.GetSByte(),
            ("byte", sbyte same) => same,
            ("byte", _) => Convert.ToSByte(value),
            ("short", null) => (short?)null,
            ("short", JsonElement el) => el.GetInt16(),
            ("short", short same) => same,
            ("short", _) => Convert.ToInt16(value),
            ("integer", null) => (int?)null,
            ("integer", JsonElement el) => el.GetInt32(),
            ("integer", int same) => same,
            ("integer", _) => Convert.ToInt32(value),
            ("long", null) => (long?)null,
            ("long", JsonElement el) => el.GetInt64(),
            ("long", long same) => same,
            ("long", _) => Convert.ToInt64(value),
            ("float", null) => (float?)null,
            ("float", JsonElement el) => el.GetSingle(),
            ("float", float same) => same,
            ("float", _) => Convert.ToSingle(value),
            ("double", null) => (double?)null,
            ("double", JsonElement el) => el.GetDouble(),
            ("double", double same) => same,
            ("double", _) => Convert.ToDouble(value),
            ("string", null) => (string?)null,
            ("string", JsonElement el) => el.GetString(),
            ("string", string same) => same,
            ("string", _) => Convert.ToString(value),
            ("boolean", null) => (bool?)null,
            ("boolean", JsonElement el) => el.GetBoolean(),
            ("boolean", bool same) => same,
            ("boolean", _) => Convert.ToBoolean(value),
            ("timestamp", null) => (DateTimeOffset?)null,
            ("timestamp", JsonElement el) => el.GetDateTimeOffset(),
            ("timestamp", DateTimeOffset dt) => dt,
            ("timestamp", _) => (DateTimeOffset)Convert.ToDateTime(value),
            _ => throw new NotImplementedException($"Unsupported type: {type}")
        };
    }

    public object? Max(string column) =>
        Log
            .Where(log => log.Add?.Stats?.MaxValues.ContainsKey(column) == true)
            .Select(log => ConvertToSchemaType(log.Add?.Stats?.MaxValues[column], column))
            .Max();

    public object? Min(string column) =>
        Log
            .Where(log => log.Add?.Stats?.MinValues.ContainsKey(column) == true)
            .Select(log => ConvertToSchemaType(log.Add?.Stats?.MinValues[column], column))
            .Min();

    public class Builder
    {
        protected IDeltaFileSystem? FileSystem = null;
        protected DeltaTable? Table = null;
        protected DeltaSchema? Schema = null;
        protected List<DeltaAction> Actions = [];

        protected bool CreateIfNotExist = false;
        protected string? Name = null;

        public Builder FromTable(DeltaTable table)
        {
            Table = table;
            FileSystem = table.FileSystem;
            Schema = table.Schema;
            return this;
        }

        public Builder WithFileSystem(IDeltaFileSystem fileSystem)
        {
            FileSystem = fileSystem;
            return this;
        }

        public Builder WithFileSystem(string path)
        {
            try
            {
                var uri = new Uri(path);
                return uri.Scheme switch
                {
                    "az" => throw new NotImplementedException(),
                    "s3" => throw new NotImplementedException(),
                    "file" => WithFileSystem(new DeltaFileSystem(uri.LocalPath)),
                    _ => throw new Exception("Unsupported file system"),
                };
            }
            catch (UriFormatException)
            {
                return WithFileSystem(new DeltaFileSystem(path));
            }
        }

        public Builder WithSchema(DeltaSchema schema)
        {
            Schema = schema;
            return this;
        }

        public Builder WithSchema(Schema schema)
        {
            Schema = DeltaSchema.FromArrow(schema);
            return this;
        }

        public Builder WithName(string name)
        {
            Name = name;
            return this;
        }

        public class SchemaBuilder
        {
            private readonly DeltaArray<DeltaSchemaField> _fields = [];
            public SchemaBuilder AddColumn(string name, string type, bool nullable)
            {
                _fields.Add(new(name, type, nullable, []));
                return this;
            }

            public DeltaSchema Build()
            {
                return new DeltaSchema("struct", _fields);
            }
        }

        public Builder WithSchema(Func<SchemaBuilder, DeltaSchema> build)
        {
            Schema = build(new SchemaBuilder());
            return this;
        }

        public sealed record AddOptions : IDisposable
        {
            public int ChunkSize { get; set; } = 1048576;
            public Compression Compression { get; set; } = Compression.Snappy;
            public Guid Id { get; set; } = Guid.NewGuid();

            // 0 = Index
            // 1 = Id
            // 2 = Compression.ToLower()
            public string PathFormat { get; set; } = "part-{0:D5}-{1}.{2}.parquet";
            public ArrowWriterPropertiesBuilder ArrowProperties { get; } = new ArrowWriterPropertiesBuilder()
                .StoreSchema();
            public WriterPropertiesBuilder ParquetProperties { get; } = new WriterPropertiesBuilder()
                .Compression(Compression.Snappy);

            public void Dispose()
            {
                ArrowProperties.Dispose();
                ParquetProperties.Dispose();
            }
        }

        public Builder Add(RecordBatch data, Action<AddOptions>? configure = null)
        {
            return Add([data], configure);
        }

        public Builder Add(IEnumerable<RecordBatch> data, Action<AddOptions>? configure = null)
        {

            if (FileSystem is null)
            {
                throw new Exception("File system is required when adding data");
            }

            if (Schema is null)
            {
                throw new Exception("Schema is required when adding data");
            }

            var enumerator = data.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                throw new ArgumentException("Data is empty");
            }

            var schema = enumerator.Current.Schema;
            using var options = new AddOptions();
            configure?.Invoke(options);
            using var arrowProperties = options.ArrowProperties.Build();
            using var parquetProperties = options.ParquetProperties.Build();

            var path = string.Format(options.PathFormat, 0, options.Id, options.Compression.ToString().ToLower());
            using var stream = FileSystem.OpenWrite(path);
            using var parquet = new FileWriter(stream, schema, parquetProperties, arrowProperties);
            var stats = new DeltaStats();
            do
            {
                stats.Update(enumerator.Current);
                parquet.WriteRecordBatch(enumerator.Current, options.ChunkSize);
            }
            while (enumerator.MoveNext());
            parquet.Close();
            stream.Close();
            var size = FileSystem.GetFileSize(path);
            return Add(path, size, DateTimeOffset.UtcNow, true, stats);
        }

        public Builder Add(string path, long size, DeltaTime timestamp, bool dataChange, DeltaStats stats)
        {
            Actions.Add(new DeltaAction(add: new(path, size, timestamp, dataChange, stats)));
            return this;
        }

        public Builder Remove(string path, bool dataChange = true)
        {
            Actions.Add(new DeltaAction(remove: new(path, dataChange)));
            return this;
        }

        public Builder EnsureCreated()
        {
            CreateIfNotExist = true;
            return this;
        }

        public DeltaTable Build()
        {


            if (FileSystem == null)
            {
                throw new Exception("File system is required");
            }


            if (!FileSystem.DirectoryExists("_delta_log"))
            {
                if (!CreateIfNotExist) throw new Exception("Table not found");
                if (Schema is null) Schema = new DeltaSchema("struct", []);
                // if (Schema is null) throw new Exception("Schema is required when creating table");
                FileSystem.CreateDirectory("_delta_log");
                WriteEntry(FileSystem, 0, [
                    new(protocol: new(1, 2, [], [])),
                    new(metaData: new(Guid.NewGuid(), Schema, DeltaFormat.Default, [], [], Name)),
                    ..Actions
                ]);
                Actions.Clear();
            }
            else if (Actions.Count > 0)
            {
                WriteEntry(FileSystem, Table!.Version + 1, Actions);
                Actions.Clear();
            }
            // if (_metaData == null)
            // {
            //     throw new Exception("Meta data is required");
            // }

            return new DeltaTable(FileSystem);

        }
    }
}


public class DeltaTable<TTable> : DeltaTable
    where TTable : ITable<TTable>
{

    internal DeltaTable(DeltaTable other) : base(other)
    {
        // Todo check schema
    }

    public async IAsyncEnumerable<TTable> ReadAll([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var batch in GetRecordBatches(cancellationToken))
        {
            foreach (var row in TTable.Enumerate(batch))
            {
                yield return row;
            }
        }
    }

    public new class Builder : DeltaTable.Builder
    {
        public new Builder FromTable(DeltaTable table) => (Builder)base.FromTable(table);
        public new Builder WithFileSystem(IDeltaFileSystem fileSystem) => (Builder)base.WithFileSystem(fileSystem);
        public new Builder WithFileSystem(string path) => (Builder)base.WithFileSystem(path);
        public new Builder WithSchema(DeltaSchema schema) => (Builder)base.WithSchema(schema);
        public new Builder WithSchema(Schema schema) => (Builder)base.WithSchema(schema);
        public new Builder WithName(string name) => (Builder)base.WithName(name);
        public new Builder EnsureCreated() => (Builder)base.EnsureCreated();
        public new Builder Add(RecordBatch data, Action<AddOptions>? configure = null) => (Builder)base.Add(data, configure);
        public new Builder Add(IEnumerable<RecordBatch> data, Action<AddOptions>? configure = null) => (Builder)base.Add(data, configure);
        public new Builder Remove(string path, bool dataChange = true) => (Builder)base.Remove(path, dataChange);
        public new DeltaTable<TTable> Build() => new(base.Build());
    }
}