using Apache.Arrow;
using Apache.Arrow.Types;

namespace DeltaLake.Tests.Unit.Arrow;

public class SchemaEqualityComparerTests
{
    public static TheoryData<Schema?, Schema?, bool> TestSchemas = new()
    {
        { null, null, true},
        { null, new Schema([], null), false },
        { new Schema([], null), null, false},
        { new Schema([], null), new Schema([], null), true },
        { new Schema([new Field("foo", Int32Type.Default, false)], null), new Schema([new Field("foo", Int32Type.Default, false)], null), true },
        { new Schema([new Field("foo", Int32Type.Default, false)], null), new Schema([], null), false },
        { new Schema([new Field("foo", Int32Type.Default, false)], null), new Schema([new Field("bar", Int32Type.Default, false)], null), false },
    };

    [Theory]
    [MemberData(nameof(TestSchemas))]
    public void Equal_WithTestSchemas(Schema? schemaX, Schema? schemaY, bool expected)
    {
        var comparer = new SchemaEqualityComparer();
        var actual = comparer.Equals(schemaX, schemaY);
        Assert.Equal(expected, actual);
        if (expected && schemaX is not null && schemaY is not null)
            Assert.Equal(comparer.GetHashCode(schemaX), comparer.GetHashCode(schemaY));
    }
}
