namespace DeltaLake.Protocol;

public record DeltaTransaction(string AppId, long Version, long? LastUpdated = null);
