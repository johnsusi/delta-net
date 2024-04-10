namespace DeltaLake.Tests.Unit;

public sealed class TestFileSystem : IDeltaFileSystem, IDisposable
{
    private readonly TempDirectory _temp = new();
    private readonly DeltaFileSystem _fs;
    public TestFileSystem() => _fs = new DeltaFileSystem(_temp.Path);
    public bool DirectoryExists(string path) => _fs.DirectoryExists(path);
    public void CreateDirectory(string path) => _fs.CreateDirectory(path);
    public bool FileExists(string path) => _fs.FileExists(path);
    public long GetFileSize(string path) => _fs.GetFileSize(path);
    public Stream OpenRead(string path) => _fs.OpenRead(path);
    public Stream OpenWrite(string path) => _fs.OpenWrite(path);
    public IEnumerable<string> ReadAllLines(string path) => _fs.ReadAllLines(path);
    public void WriteFile(string path, IEnumerable<string> content) => _fs.WriteFile(path, content);
    public string CreateTempFile() => _fs.CreateTempFile();
    public void Dispose() => _temp.Dispose();
    public bool MoveFile(string source, string destination) => _fs.MoveFile(source, destination);
}
