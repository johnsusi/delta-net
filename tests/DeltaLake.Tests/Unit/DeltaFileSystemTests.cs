namespace DeltaLake.Tests.Unit;

public class DeltaFileSystemTests
{
    [Fact]
    public void DirectoryExists_WithMissingDirectory_ShouldReturnFalse()
    {
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        var exists = fileSystem.DirectoryExists("missing");

        Assert.False(exists);
    }

    [Fact]
    public void DirectoryExists_WithExistingDirectory_ShouldReturnTrue()
    {
        using var temp = new TempDirectory();
        Directory.CreateDirectory(Path.Combine(temp.Path, "existing"));

        var fileSystem = new DeltaFileSystem(temp.Path);

        var exists = fileSystem.DirectoryExists("existing");

        Assert.True(exists);
    }

    [Fact]
    public void CreateDirectory_WithMissingDirectory_ShouldCreate()
    {
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        fileSystem.CreateDirectory("missing");

        Assert.True(Directory.Exists(Path.Combine(temp.Path, "missing")));
    }

    [Fact]
    public void CreateDirectory_WithExistingDirectory_ShouldDoNothing()
    {
        using var temp = new TempDirectory();
        Directory.CreateDirectory(Path.Combine(temp.Path, "existing"));
        var fileSystem = new DeltaFileSystem(temp.Path);
        fileSystem.CreateDirectory("existing");

        Assert.True(Directory.Exists(Path.Combine(temp.Path, "existing")));
    }

    [Fact]
    public void FileExists_WithMissingFile_ShouldReturnFalse()
    {
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        var exists = fileSystem.FileExists("missing");

        Assert.False(exists);
    }

    [Fact]
    public void FileExists_WithExistingFile_ShouldReturnTrue()
    {
        using var temp = new TempDirectory();
        File.WriteAllText(Path.Combine(temp.Path, "existing"), "test");
        var fileSystem = new DeltaFileSystem(temp.Path);

        var exists = fileSystem.FileExists("existing");

        Assert.True(exists);
    }

    [Fact]
    public void ReadAllLines_WithExistingFile_ShouldReturnLines()
    {
        string[] expected = ["line1", "line2"];
        using var temp = new TempDirectory();
        File.WriteAllLines(Path.Combine(temp.Path, "existing"), expected);
        var fileSystem = new DeltaFileSystem(temp.Path);

        var actual = fileSystem.ReadAllLines("existing");

        Assert.Equal(actual, expected);
    }

    [Fact]
    public void WriteFile_WithMissingFile_ShouldCreate()
    {
        string[] content = ["line1", "line2"];
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        fileSystem.WriteFile("missing", content);

        Assert.Equal(content, File.ReadAllLines(Path.Combine(temp.Path, "missing")));
    }

    [Fact]
    public void WriteFile_WithExistingFile_ShouldOverwrite()
    {
        string[] content = ["line1", "line2"];
        using var temp = new TempDirectory();
        File.WriteAllLines(Path.Combine(temp.Path, "existing"), ["line0"]);
        var fileSystem = new DeltaFileSystem(temp.Path);

        fileSystem.WriteFile("existing", content);

        Assert.Equal(content, File.ReadAllLines(Path.Combine(temp.Path, "existing")));
    }

    [Fact]
    public void CreateTempFile_ShouldCreate()
    {
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        var tempFile = fileSystem.CreateTempFile();

        Assert.True(File.Exists(Path.Combine(temp.Path, tempFile)));
    }

    [Fact]
    public void MoveFile_WithMissingSource_ShouldReturnFalse()
    {
        using var temp = new TempDirectory();
        var fileSystem = new DeltaFileSystem(temp.Path);

        var moved = fileSystem.MoveFile("missing", "destination");

        Assert.False(moved);
    }

    [Fact]
    public void MoveFile_WithExistingSource_ShouldMove()
    {
        using var temp = new TempDirectory();
        File.WriteAllText(Path.Combine(temp.Path, "source"), "test");
        var fileSystem = new DeltaFileSystem(temp.Path);

        var moved = fileSystem.MoveFile("source", "destination");

        Assert.True(moved);
        Assert.False(File.Exists(Path.Combine(temp.Path, "source")));
        Assert.True(File.Exists(Path.Combine(temp.Path, "destination")));
    }

    [Fact]
    public void MoveFile_WithExistingDestination_ShouldReturnFalse()
    {
        using var temp = new TempDirectory();
        File.WriteAllText(Path.Combine(temp.Path, "source"), "source");
        File.WriteAllText(Path.Combine(temp.Path, "destination"), "dest");
        var fileSystem = new DeltaFileSystem(temp.Path);

        var moved = fileSystem.MoveFile("source", "destination");

        Assert.False(moved);
        Assert.Equal("dest", File.ReadAllText(Path.Combine(temp.Path, "destination")));

    }
}
