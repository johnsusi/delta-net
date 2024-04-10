namespace DeltaLake;

public interface IDeltaFileSystem
{
    public bool DirectoryExists(string path);
    public void CreateDirectory(string path);
    public bool FileExists(string path);
    public long GetFileSize(string path);
    public Stream OpenRead(string path);
    public Stream OpenWrite(string path);
    public IEnumerable<string> ReadAllLines(string path);
    public void WriteFile(string path, IEnumerable<string> content);

    public string CreateTempFile();
    public bool MoveFile(string source, string destination);
}
