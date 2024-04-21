using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Protocol;
using ParquetSharp;

namespace DeltaLake.Tests.Unit;

public class DeltaTableTests
{

    // [Fact]
    // public void New_WithMissingTable_ShouldCreate()
    // {
    //     using var fs = new TestFileSystem();

    //     var deltaTable = new DeltaTable(fs, createIfNotExist: true);

    //     Assert.True(fs.FileExists("_delta_log/00000000000000000000.json"));
    // }

    // [Fact]
    // public void New_WithMissingTable_ShouldThrow()
    // {
    //     using var fs = new TestFileSystem();

    //     Assert.Throws<Exception>(() => new DeltaTable(fs, createIfNotExist: false));
    // }

    // [Fact]
    // public void New_WithMissingTable_ShouldCreateTable()
    // {
    //     using var fs = new TestFileSystem();

    //     DeltaTable.Builder.(fs, createIfNotExist: true);

    //     Assert.True(fs.FileExists("_delta_log/00000000000000000000.json"));

    // }

    [Fact]
    public void Build_WithExistingTable_ShouldReturnTable()
    {
        using var fs = new TestFileSystem();
        var metaData = new DeltaMetaData(Guid.NewGuid(), new DeltaSchema("struct", []), DeltaFormat.Default, [], []);
        fs.CreateDirectory("_delta_log");
        fs.WriteFile("_delta_log/00000000000000000000.json", [
            JsonSerializer.Serialize(new DeltaAction(metaData), DeltaAction.JsonSerializerOptions),
        ]);

        var deltaTable = new DeltaTable.Builder().WithFileSystem(fs).Build();

        Assert.Equal(metaData, deltaTable.MetaData);
    }


    [Fact]
    public void Max_WithExistingData_ShouldReturnMax()
    {
        using var fs = new TestFileSystem();
        var table = new DeltaTable(
            new TestFileSystem(),
            [
                new (metaData: new(
                    Guid.NewGuid(),
                    new DeltaSchema("struct", [
                        new("Test", "integer", false, []),
                    ]),
                    DeltaFormat.Default,
                    [],
                    []
                )),
                new DeltaAction(add: new ("part-00000-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, new DeltaStats(3, [new("Test", 1)],  [new("Test", 3)], [new ("Test", 0)])))
            ],
            0
        );


        var max = (int?)table.Max("Test");

        Assert.NotNull(max);
        Assert.Equal(3, max);
    }

    [Fact]
    public void Max_WithMultipleAdds_ShouldReturnMax()
    {
        var tsType = new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, TimeZoneInfo.Utc);
        var ts = Enumerable
            .Range(0, 10)
            .Select(i => DateTimeOffset.FromUnixTimeMilliseconds(i))
            .ToArray();
        var schema = new Schema([
            new("TestByte", Int8Type.Default, true),
            new("TestShort", Int16Type.Default, true),
            new("TestInteger", Int32Type.Default, true),
            new("TestLong", Int64Type.Default, true),
            new("TestFloat", FloatType.Default, true),
            new("TestDouble", DoubleType.Default, true),
            new("TestString", StringType.Default, true),
            new("TestBoolean", BooleanType.Default, true),
            new("TestTimestamp", tsType, true),
        ], []);
        using var fs = new TestFileSystem();

        var s1 = new DeltaStats(3, [
        ], [
        ], [
            new ("TestByte", 3),
            new ("TestShort", 3),
            new ("TestInteger", 3),
            new ("TestLong", 3),
            new ("TestFloat", 3),
            new ("TestDouble", 3),
            new ("TestString", 3),
            new ("TestBoolean", 3),
            new ("TestTimestamp", 3),
        ]);

        var s2 = new DeltaStats(3, [
            new ("TestByte", (sbyte)4),
            new ("TestShort", (short)4),
            new ("TestInteger", 4),
            new ("TestLong", 4L),
            new ("TestFloat", 4F),
            new ("TestDouble", 4D),
            new ("TestString", "4"),
            new ("TestBoolean", false),
            new ("TestTimestamp", ts[4]),

        ], [
            new ("TestByte", (sbyte)9),
            new ("TestShort", (short)9),
            new ("TestInteger", 9),
            new ("TestLong", 9L),
            new ("TestFloat", 9F),
            new ("TestDouble", 9D),
            new ("TestString", "9"),
            new ("TestBoolean", true),
            new ("TestTimestamp", ts[9]),
        ], [
            new ("TestByte", 0),
            new ("TestShort", 0),
            new ("TestInteger", 0),
            new ("TestLong", 0),
            new ("TestFloat", 0),
            new ("TestDouble", 0),
            new ("TestString", 0),
            new ("TestBoolean", 0),
            new ("TestTimestamp", 0),
        ]);



        var s3 = new DeltaStats(3, [
            new ("TestByte", (sbyte)1),
            new ("TestShort", (short)1),
            new ("TestInteger", 1),
            new ("TestLong", 1L),
            new ("TestFloat", 1F),
            new ("TestDouble", 1D),
            new ("TestString", "1"),
            new ("TestBoolean", false),
            new ("TestTimestamp", ts[1]),

        ], [
            new ("TestByte", (sbyte)3),
            new ("TestShort", (short)3),
            new ("TestInteger", 3),
            new ("TestLong", 3L),
            new ("TestFloat", 3F),
            new ("TestDouble", 3D),
            new ("TestString", "3"),
            new ("TestBoolean", true),
            new ("TestTimestamp", ts[3]),
        ], [
            new ("TestByte", 0),
            new ("TestShort", 0),
            new ("TestInteger", 0),
            new ("TestLong", 0),
            new ("TestFloat", 0),
            new ("TestDouble", 0),
            new ("TestString", 0),
            new ("TestBoolean", 0),
            new ("TestTimestamp", 0),
        ]);
        var table = new DeltaTable(
            new TestFileSystem(),
            [
                new (metaData: new(
                    Guid.NewGuid(),
                    DeltaSchema.FromArrow(schema),
                    DeltaFormat.Default,
                    [],
                    []
                )),
                new DeltaAction(add: new ("part-00000-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, s1)),
                new DeltaAction(add: new ("part-00001-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, s2)),
                new DeltaAction(add: new ("part-00002-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, s3)),
            ],
            0
        );

        var max = table.Max("Test");

        Assert.Equal((sbyte)1, table.Min("TestByte"));
        Assert.Equal((short)1, table.Min("TestShort"));
        Assert.Equal(1, table.Min("TestInteger"));
        Assert.Equal(1L, table.Min("TestLong"));
        Assert.Equal(1F, table.Min("TestFloat"));
        Assert.Equal(1D, table.Min("TestDouble"));
        Assert.Equal("1", table.Min("TestString"));
        Assert.Equal(false, table.Min("TestBoolean"));
        Assert.Equal(ts[1], table.Min("TestTimestamp"));

        Assert.Equal((sbyte)9, table.Max("TestByte"));
        Assert.Equal((short)9, table.Max("TestShort"));
        Assert.Equal(9, table.Max("TestInteger"));
        Assert.Equal(9L, table.Max("TestLong"));
        Assert.Equal(9F, table.Max("TestFloat"));
        Assert.Equal(9D, table.Max("TestDouble"));
        Assert.Equal("9", table.Max("TestString"));
        Assert.Equal(true, table.Max("TestBoolean"));
        Assert.Equal(ts[9], table.Max("TestTimestamp"));
    }


    [Fact]
    public void MinMax_WithTableFromFileSystem_ShouldReturnMax()
    {
        var tsType = new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, TimeZoneInfo.Utc);
        var ts = Enumerable
            .Range(0, 10)
            .Select(i => DateTimeOffset.FromUnixTimeMilliseconds(i))
            .ToArray();
        using var fs = new TestFileSystem();
        var schema = new Schema([
            new("TestByte", Int8Type.Default, true),
            new("TestShort", Int16Type.Default, true),
            new("TestInteger", Int32Type.Default, true),
            new("TestLong", Int64Type.Default, true),
            new("TestFloat", FloatType.Default, true),
            new("TestDouble", DoubleType.Default, true),
            new("TestString", StringType.Default, true),
            new("TestBoolean", BooleanType.Default, true),
            new("TestTimestamp", tsType, true),
        ], []);
        using var data1 = new RecordBatch(schema, [
            new Int8Array.Builder().AppendNull().Build(),
            new Int16Array.Builder().AppendNull().Build(),
            new Int32Array.Builder().AppendNull().Build(),
            new Int64Array.Builder().AppendNull().Build(),
            new FloatArray.Builder().AppendNull().Build(),
            new DoubleArray.Builder().AppendNull().Build(),
            new StringArray.Builder().AppendNull().Build(),
            new BooleanArray.Builder().AppendNull().Build(),
            new TimestampArray.Builder(tsType).AppendNull().Build(),
        ], 1);
        using var data2 = new RecordBatch(schema, [
            new Int8Array.Builder().AppendRange([4, 6, 5]).Build(),
            new Int16Array.Builder().AppendRange([4, 6, 5]).Build(),
            new Int32Array.Builder().AppendRange([4, 6, 5]).Build(),
            new Int64Array.Builder().AppendRange([4, 6, 5]).Build(),
            new FloatArray.Builder().AppendRange([4, 6, 5]).Build(),
            new DoubleArray.Builder().AppendRange([4, 6, 5]).Build(),
            new StringArray.Builder().AppendRange(["4", "6", "5"]).Build(),
            new BooleanArray.Builder().AppendRange([true, false, true]).Build(),
            new TimestampArray.Builder(tsType).AppendRange([ts[4], ts[6], ts[5]]).Build(),
        ], 3);
        using var data3 = new RecordBatch(schema, [
            new Int8Array.Builder().AppendRange([1, 2, 3]).Build(),
            new Int16Array.Builder().AppendRange([1, 2, 3]).Build(),
            new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
            new Int64Array.Builder().AppendRange([1, 2, 3]).Build(),
            new FloatArray.Builder().AppendRange([1, 2, 3]).Build(),
            new DoubleArray.Builder().AppendRange([1, 2, 3]).Build(),
            new StringArray.Builder().AppendRange(["1", "2", "3"]).Build(),
            new BooleanArray.Builder().AppendRange([true, false, true]).Build(),
            new TimestampArray.Builder(tsType).AppendRange([ts[1], ts[2], ts[3]]).Build(),
        ], 3);

        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .EnsureCreated()
            .Add(data1)
            .Add(data2)
            .Add(data3)
            .Build();

        Assert.Equal((sbyte)1, table.Min("TestByte"));
        Assert.Equal((short)1, table.Min("TestShort"));
        Assert.Equal(1, table.Min("TestInteger"));
        Assert.Equal(1L, table.Min("TestLong"));
        Assert.Equal(1F, table.Min("TestFloat"));
        Assert.Equal(1D, table.Min("TestDouble"));
        Assert.Equal("1", table.Min("TestString"));
        Assert.Equal(false, table.Min("TestBoolean"));
        Assert.Equal(ts[1], table.Min("TestTimestamp"));

        Assert.Equal((sbyte)6, table.Max("TestByte"));
        Assert.Equal((short)6, table.Max("TestShort"));
        Assert.Equal(6, table.Max("TestInteger"));
        Assert.Equal(6L, table.Max("TestLong"));
        Assert.Equal(6F, table.Max("TestFloat"));
        Assert.Equal(6D, table.Max("TestDouble"));
        Assert.Equal("6", table.Max("TestString"));
        Assert.Equal(true, table.Max("TestBoolean"));
        Assert.Equal(ts[6], table.Max("TestTimestamp"));

    }

    [Fact]
    public void Name_WithNamedTable_ShouldReturnName()
    {
        string expected = "TestTableName";
        using var fs = new TestFileSystem();
        var table = new DeltaTable(
            new TestFileSystem(),
            [
                new (metaData: new(
                    Guid.NewGuid(),
                    new DeltaSchema("struct", [
                        new("Test", "integer", false, []),
                    ]),
                    DeltaFormat.Default,
                    [],
                    [],
                    expected
                ))
            ],
            0
        );

        var actual = table.Name;

        Assert.Equal(expected, actual);
    }


    [Fact]
    public void Name_WithNamedTableFromFileSystem_ShouldReturnName()
    {
        string expected = "TestTableName";
        var schema = new Schema([
        ], []);
        using var fs = new TestFileSystem();
        var table = new DeltaTable.Builder()
            .WithFileSystem(new TestFileSystem())
            .WithSchema(schema)
            .WithName(expected)
            .EnsureCreated()
            .Build();

        var actual = table.Name;

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Builder_Add_ShouldWriteSize()
    {
        using var fs = new TestFileSystem();
        var schema = new Schema([
            new("Test", Int32Type.Default, false),
        ], []);
        using var data = new RecordBatch(schema, [
            new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
        ], 3);
        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .EnsureCreated()
            .Add(data)
            .Build();


        var sizeInLog = table.Log.Where(log => log.Add is not null).First().Add!.Size;
        var sizeOnDisk = fs.GetFileSize(table.Files[0]);

        Assert.Equal(sizeInLog, sizeOnDisk);
    }

}
