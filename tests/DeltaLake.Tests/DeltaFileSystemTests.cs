namespace DeltaLake.Tests;

public class DeltaFileSystemTests
{
    [Fact]
    public void New_CreatesFileSystem()
    {
        var fileSystem = new DeltaFileSystem();
        Assert.NotNull(fileSystem);
    }
}
