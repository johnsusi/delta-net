namespace DeltaLake.Tests.Unit;

public class TempDirectory : IDisposable
{

    private readonly DirectoryInfo _info = Directory.CreateTempSubdirectory();

    public string Path => _info.FullName;

    public void Dispose() => _info.Delete(true);

}
