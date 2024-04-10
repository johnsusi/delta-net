namespace DeltaLake;

public class DeltaFileSystem : IDeltaFileSystem
{
    public string RootPath { get; }
    public DeltaFileSystem(string path) => RootPath = path;
    public bool DirectoryExists(string path) => Directory.Exists(Path.Combine(RootPath, path));
    public void CreateDirectory(string path) => Directory.CreateDirectory(Path.Combine(RootPath, path));
    public bool FileExists(string path) => File.Exists(Path.Combine(RootPath, path));
    public long GetFileSize(string path) => new FileInfo(Path.Combine(RootPath, path)).Length;
    public Stream OpenRead(string path) => File.OpenRead(Path.Combine(RootPath, path));
    public Stream OpenWrite(string path) => File.OpenWrite(Path.Combine(RootPath, path));
    public IEnumerable<string> ReadAllLines(string path) => File.ReadAllLines(Path.Combine(RootPath, path));
    public void WriteFile(string path, IEnumerable<string> content) => File.WriteAllLines(Path.Combine(RootPath, path), content);
    public string CreateTempFile()
    {
        var fileName = $".{Guid.NewGuid()}";
        File.WriteAllText(Path.Combine(RootPath, fileName), "");
        return fileName;
    }
    public bool MoveFile(string source, string destination)
    {
        try
        {
            File.Move(Path.Combine(RootPath, source), Path.Combine(RootPath, destination), false);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }
}
