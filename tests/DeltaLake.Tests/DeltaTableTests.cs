namespace DeltaLake.Tests;

public class DeltaTableTests
{
    [Fact]
    public void New_CreatesTable()
    {
        var table = new DeltaTable();
        Assert.NotNull(table);
    }
}