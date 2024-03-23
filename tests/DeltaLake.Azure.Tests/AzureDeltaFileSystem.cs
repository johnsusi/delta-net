namespace DeltaLake.Azure.Tests;

public class DeltaFileSystemTests
{
    [Fact]
    public void New_CreatesFileSystem()
    {
        var fileSystem = new AzureDeltaFileSystem();
        Assert.NotNull(fileSystem);
    }
}
