using System.Text.Json;
using Apache.Arrow;
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

    // private static void CreateTestTable(IDeltaFileSystem fs)
    // {

    //     var ids = new int[] { 1, 2, 3 };
    //     var timestamps = new DateTimeOffset[] {
    //         new(2021, 1, 1, 0, 0, 0, TimeSpan.Zero),
    //         new(2021, 1, 2, 0, 0, 0, TimeSpan.Zero),
    //         new(2021, 1, 3, 0, 0, 0, TimeSpan.Zero),
    //     };

    //     var numericValues = new double[] { 0, 2, 1 };
    //     var stringValues = new string[] { "a", "c", "b" };

    //     var columns = new Column[]
    //     {
    //         new Column<int>("Id"),
    //         new Column<DateTimeOffset>("Timestamp"),
    //         new Column<double>("numericValue"),
    //         new Column<string>("numericValue")
    //     };

    //     var filename = "part-00000-{Guid.NewGuid()}.parquet";
    //     using var stream = fs.OpenWrite(filename);
    //     using var file = new ParquetFileWriter(stream, columns);
    //     using var rowGroup = file.AppendRowGroup();

    //     using (var writer = rowGroup.NextColumn().LogicalWriter<int>())
    //     {
    //         writer.WriteBatch(ids);
    //     }
    //     using (var writer = rowGroup.NextColumn().LogicalWriter<DateTimeOffset>())
    //     {
    //         writer.WriteBatch(timestamps);
    //     }
    //     using (var writer = rowGroup.NextColumn().LogicalWriter<double>())
    //     {
    //         writer.WriteBatch(numericValues);
    //     }
    //     using (var writer = rowGroup.NextColumn().LogicalWriter<string>())
    //     {
    //         writer.WriteBatch(stringValues);
    //     }

    //     file.Close();

    //     var metaData = new DeltaMetaData(Guid.NewGuid(), new DeltaSchema("struct", []), DeltaFormat.Default, [], []);
    //     var add = new DeltaAdd(filename, 0, DateTimeOffset.UtcNow, true);
    //     fs.CreateDirectory("_delta_log");
    //     fs.WriteFile("_delta_log/00000000000000000000.json", [
    //         JsonSerializer.Serialize(new DeltaAction(metaData: metaData), DeltaAction.JsonSerializerOptions),
    //         JsonSerializer.Serialize(new DeltaAction(add: add), DeltaAction.JsonSerializerOptions),
    //     ]);
    // }

}
