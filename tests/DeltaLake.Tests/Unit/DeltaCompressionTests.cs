namespace DeltaLake.Tests.Unit;

public class DeltaCompressionTests
{
    [Fact]
    public void ToParquetSharpCompression_WithAllCompressionValues_ShouldReturnEquivalentParquetSharpCompression()
    {
        // Arrange
        var expected = Enum.GetValues(typeof(ParquetSharp.Compression));
        var sourceEnumValues = Enum.GetValues(typeof(DeltaCompression)).Cast<DeltaCompression>();
        
        // Act
        var convertedEnums = sourceEnumValues.Select(i => i.ToParquetSharpCompression());
        
        // Assert
        Assert.Equivalent(expected, convertedEnums);
    }
}