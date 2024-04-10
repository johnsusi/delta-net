using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaFormatTests
{

    [Fact]
    public void Provider_ShouldReturnProvider()
    {
        DeltaFormat deltaFormat = DeltaFormat.Default;

        var provider = deltaFormat.Provider;

        Assert.Equal("parquet", provider);
    }

    [Fact]
    public void Options_ShouldReturnOptions()
    {
        DeltaFormat deltaFormat = DeltaFormat.Default;

        var options = deltaFormat.Options;

        Assert.NotNull(options);
    }

    [Fact]
    public void Options_ShouldBeEmpty()
    {
        DeltaFormat deltaFormat = DeltaFormat.Default;

        var options = deltaFormat.Options;

        Assert.Empty(options);
    }

    [Fact]
    public void Equals_ShouldReturnTrue()
    {
        DeltaFormat deltaFormat1 = new("parquet", new() { ["key"] = "value" });
        DeltaFormat deltaFormat2 = new("parquet", new() { ["key"] = "value" });

        var equals = deltaFormat1.Equals(deltaFormat2);

        Assert.True(equals);
    }

    [Fact]
    public void Equals_ShouldReturnFalse()
    {
        DeltaFormat deltaFormat1 = new("parquet", []);
        DeltaFormat deltaFormat2 = new("parquet", new() { ["key"] = "value" });

        var equals = deltaFormat1.Equals(deltaFormat2);

        Assert.False(equals);
    }

    [Fact]
    public void GetHashCode_ShouldReturnSame()
    {
        DeltaFormat deltaFormat1 = new("parquet", new() { ["key"] = "value" });
        DeltaFormat deltaFormat2 = new("parquet", new() { ["key"] = "value" });

        var equals = deltaFormat1.GetHashCode().Equals(deltaFormat2.GetHashCode());

        Assert.True(equals);
    }

}
