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
                new DeltaAction(add: new ("part-00000-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, new DeltaStats(3, [new("Test", 1)],  [new("Test", 3)], [new ("Test", 0)]))),
                new DeltaAction(add: new ("part-00001-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, new DeltaStats(3, [new("Test", 6)],  [new("Test", 9)], [new ("Test", 0)]))),
                new DeltaAction(add: new ("part-00002-00000000-0000-0000-0000-000000.parquet", 0, DateTimeOffset.UtcNow, true, new DeltaStats(3, [new("Test", 4)],  [new("Test", 5)], [new ("Test", 0)]))),
            ],
            0
        );

        var max = table.Max("Test");

        Assert.Equal(9, max);
    }


    [Fact]
    public void Max_WithTableFromFileSystem_ShouldReturnMax()
    {
        using var fs = new TestFileSystem();
        var schema = new Schema([new("Test", Int32Type.Default, false)], []);
        using var data1 = new RecordBatch(schema, [new Int32Array.Builder().AppendRange([1, 2, 3]).Build()], 3);
        using var data2 = new RecordBatch(schema, [new Int32Array.Builder().AppendRange([7, 8, 9]).Build()], 3);
        using var data3 = new RecordBatch(schema, [new Int32Array.Builder().AppendRange([4, 5, 6]).Build()], 3);
        var table = new DeltaTable.Builder()
            .WithFileSystem(fs)
            .WithSchema(schema)
            .EnsureCreated()
            .Add(data1)
            .Add(data2)
            .Add(data3)
            .Build();

        var max = table.Max("Test");

        Assert.Equal(9, max);
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
        var schema = new Schema([new("Test", Int32Type.Default, false)], []);
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

}
