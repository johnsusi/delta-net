using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Protocol;
using DeltaLake.Tests.Unit;

namespace DeltaLake.Tests.Functional;

public class CreateTableTests
{

    public static Schema TestSchema = new Schema([
            new("id", Int32Type.Default, false, []),
            new("name", StringType.Default, true, [])
        ], []);

    internal readonly record struct TestTable(int Id, string? Name) : ITable<TestTable>
    {
        public static Schema Schema => TestSchema;

        public static IEnumerable<TestTable> Enumerate(RecordBatch batch)
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var idArray = batch.Column(0) as IReadOnlyList<int?> ?? throw new Exception("Expected non-null array");
                var nameArray = batch.Column(1) as IReadOnlyList<string?> ?? throw new Exception("Expected non-null array");
                yield return new TestTable()
                {
                    Id = idArray[i] ?? throw new Exception("Cannot be null"),
                    Name = nameArray[i]
                };
            }
        }
    }

    [Fact]
    public void CreateEmptyTable()
    {
        using var fs = new TestFileSystem();

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .EnsureCreated()
            .Build();

        Assert.NotNull(table);
    }

    [Fact]
    public void CreateTableWithSchema()
    {
        using var fs = new TestFileSystem();

        var expected = new DeltaSchema("struct", [
            new("id", "integer", false, []),
            new("name", "string", true, [])
        ]);

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(expected)
            .EnsureCreated()
            .Build();

        Assert.Equal(expected, table.Schema);
    }

    [Fact]
    public void CreateTableWithData()
    {
        using var fs = new TestFileSystem();
        var schema = new Schema([
            new("id", Int32Type.Default, false, []),
            new("name", StringType.Default, true, [])
        ], []);
        using var expected = new RecordBatch(schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .Add(expected.Clone())
            .EnsureCreated()
            .Build();
        var actual = table.GetRecordBatches();

        Assert.Equal([expected], actual, new RecordBatchEqualityComparer());
    }

    [Fact]
    public void FileSystemIsRequired()
    {
        using var fs = new TestFileSystem();

        var builder = new DeltaTable.Builder()
            .WithSchema(TestSchema);

        Assert.Throws<Exception>(() => builder.Build());
    }

    [Fact]
    public void SchemaIsRequired()
    {
        using var fs = new TestFileSystem();

        var builder = new DeltaTable.Builder()
            .WithFileSystem(fs);

        Assert.Throws<Exception>(() => builder.Build());
    }

    [Fact]
    public void RemovingDataFromTable()
    {
        using var fs = new TestFileSystem();
        var schema = new Schema([
            new("id", Int32Type.Default, false, []),
            new("name", StringType.Default, true, [])
        ], []);
        using var file1 = new RecordBatch(schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);
        using var file2 = new RecordBatch(schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .Add(file1.Clone())
            .Add(file2.Clone())
            .EnsureCreated()
            .Build();

        table = new DeltaTable.Builder()
            .FromTable(table)
            .Remove(table.Files[0])
            .Build();

        Assert.Equal([file1], table.GetRecordBatches(), new RecordBatchEqualityComparer());
    }

    [Fact]
    public void ReadingFromTypedTable()
    {
        using var fs = new TestFileSystem();

        using var data = new RecordBatch(TestTable.Schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);


        var table = new DeltaTable<TestTable>.Builder()
            .WithFileSystem(fs)
            .WithSchema(TestTable.Schema)
            .Add(data)
            .EnsureCreated()
            .Build();

        var actual = table.ReadAll();

        Assert.NotEmpty(actual); ;

    }

    [Fact]
    public async void AppendTable()
    {
        using var fs = new TestFileSystem();
        using var data1 = new RecordBatch(TestTable.Schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);
        using var data2 = new RecordBatch(TestTable.Schema, [
            new Int32Array.Builder().Append(4).Append(5).Append(6).Build(),
            new StringArray.Builder().Append("four").AppendNull().Append("six").Build()
        ], 3);
        var table1 = new DeltaTable<TestTable>.Builder()
            .WithFileSystem(fs)
            .WithSchema(TestTable.Schema)
            .Add(data1)
            .EnsureCreated()
            .Build();

        var table2 = new DeltaTable<TestTable>.Builder()
            .FromTable(table1)
            .Add(data2)
            .Build();
        var d1 = await table1.ReadAll().ToListAsync();
        var d2 = await table2.ReadAll().ToListAsync();

        Assert.Equal(table1.Id, table2.Id);
        Assert.NotEqual(table1.Version, table2.Version);
        Assert.NotEqual(d1, d2);
        Assert.Empty(d1.Except(d2));

    }

    [Fact]
    public void ChangeTable()
    {
        using var fs = new TestFileSystem();
        using var data1 = new RecordBatch(TestTable.Schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
        ], 3);
        using var data2 = new RecordBatch(TestTable.Schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").Append("two").Append("six").Build()
        ], 3);
        var table1 = new DeltaTable<TestTable>.Builder()
            .WithFileSystem(fs)
            .WithSchema(TestTable.Schema)
            .Add(data1.Clone())
            .EnsureCreated()
            .Build();

        var table2 = new DeltaTable<TestTable>.Builder()
            .FromTable(table1)
            .Remove(table1.Files[0])
            .Add(data2.Clone())
            .Build();

        Assert.Equal([data2], table2.GetRecordBatches(), new RecordBatchEqualityComparer());
    }

    [Fact]
    public void StatsShouldRespectSchemaTypes()
    {
        using var fs = new TestFileSystem();
        var tsType = new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc);
        var tsMin = DateTimeOffset.FromUnixTimeMilliseconds(0);
        var tsMax = DateTimeOffset.FromUnixTimeMilliseconds(1);
        var schema = new Schema([
            new("id", Int32Type.Default, false, []),
            new("name", StringType.Default, true, []),
            new("when", tsType, false, []),
        ], []);
        using var batch = new RecordBatch(schema, [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("one").AppendNull().Append("two").Build(),
            new TimestampArray.Builder(tsType).Append(tsMin).AppendNull().Append(tsMax).Build()
        ], 3);

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .Add(batch)
            .EnsureCreated()
            .Build();

        var minId = table.Min("id");
        var maxId = table.Max("id");
        var minName = table.Min("name");
        var maxName = table.Max("name");
        var minWhen = table.Min("when");
        var maxWhen = table.Max("when");

        Assert.Equal(1, minId);
        Assert.Equal(3, maxId);
        Assert.Equal("one", minName);
        Assert.Equal("two", maxName);
        Assert.Equal(tsMin, minWhen);
        Assert.Equal(tsMax, maxWhen);


    }

}
