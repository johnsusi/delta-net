using Apache.Arrow;
using Apache.Arrow.Types;

namespace DeltaLake.Tests.Unit.Arrow;
public class FieldEqualityComparerTests
{

    public static TheoryData<IArrowType, bool, IEnumerable<KeyValuePair<string, string>>?> TestTypes = new()
    {
        { new BooleanType(), true, null },
        { new BooleanType(), true, null },
        { new Int8Type(), true, null },
        { new Int16Type(), true, null },
        { new Int32Type(), true, null },
        { new Int64Type(), true, null },
        { new UInt8Type(), true, null },
        { new UInt16Type(), true, null },
        { new UInt32Type(), true, null },
        { new UInt64Type(), true, null },
        { new StringType(), true, null },
        { new StructType([new Field("field", new Int32Type(), true)]), true, null },
        { new ListType(new Int32Type()), true, null },
        { new DictionaryType(new Int32Type(), new StringType(), ordered: true), true, null },
        { new DictionaryType(new Int32Type(), new StringType(), ordered: false), true, null },
        { new Date32Type(), true, null },
        { new Date64Type(), true, null },
        { new Time32Type(TimeUnit.Second), true, null },
        { new Time64Type(TimeUnit.Microsecond), true, null },
        { new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc), true, null },
        // { new DurationType(TimeUnit.Microsecond), true, null },
        { new Decimal128Type(10, 2), true, null },
        { new Decimal256Type(10, 2), true, null },
        { new FixedSizeBinaryType(10), true, null },
        // { new FixedSizeListType(10, new Int32Type()), true, null },
        // { new UnionType(new Field("field", new Int32Type())), true, null },
        { new NullType(), true, null },
        { new BinaryType(), true, null },
        // { new LargeBinaryType(), true, null },
        // { new LargeStringType(), true, null },
        // { new LargeListType(new Int32Type()), true, null },
        // { new LargeStructType(new Field("field", new Int32Type())), true, null },
        // { new LargeUnionType(new Field("field", new Int32Type())), true, null },
        { new MapType(new Int32Type(), new StringType()), true, null },

        { new BooleanType(), false, [] },
        { new BooleanType(), false, [new ("foo", "bar")] },
        { new Int8Type(), false, [new ("foo", "bar"), new ("baz", "inga")] },


    };

    [Fact]
    public void Equals_Null_Null()
    {
        var comparer = new FieldEqualityComparer();
        Assert.True(comparer.Equals(null, null));
    }

    [Theory]
    [MemberData(nameof(TestTypes))]
    public void Equals_Null_NotNull(IArrowType type, bool nullable, IEnumerable<KeyValuePair<string, string>>? metadata)
    {
        var comparer = new FieldEqualityComparer();
        Assert.False(comparer.Equals(null, new Field("field", type, nullable, metadata)));
    }

    [Theory]
    [MemberData(nameof(TestTypes))]
    public void Equals_NotNull_Null(IArrowType type, bool nullable, IEnumerable<KeyValuePair<string, string>>? metadata)
    {
        var comparer = new FieldEqualityComparer();
        Assert.False(comparer.Equals(new Field("field", type, nullable, metadata), null));
    }

    [Theory]
    [MemberData(nameof(TestTypes))]
    public void Equals_SameField_True(IArrowType type, bool nullable, IEnumerable<KeyValuePair<string, string>>? metadata)
    {
        var fieldX = new Field("field", type, nullable, metadata);
        var fieldY = new Field("field", type, nullable, metadata);
        var comparer = new FieldEqualityComparer();
        Assert.True(comparer.Equals(fieldX, fieldY));
    }


    // [Fact]
    // public void Equals_DifferentDataType_False()
    // {
    //     var types = TestTypes().SelectMany(t => t.Select(t2 => (IArrowType)t2)).ToArray();
    //     var comparer = new FieldEqualityComparer();
    //     for (int i = 0; i < types.Length; ++i)
    //     {
    //         for (int j = 0; j < types.Length; ++j)
    //         {
    //             if (i == j) continue;
    //             foreach (var nullable in new bool[] { true, false })
    //                 Assert.False(comparer.Equals(new Field("field", types[i], nullable), new Field("field", types[j], nullable)));
    //         }
    //     }
    // }

    // [Fact]
    // public void GetHashCode_SameField_SameHashCode()
    // {
    //     var field = new Field("field", new Int32DataType());
    //     var comparer = new FieldEqualityComparer();
    //     Assert.Equal(comparer.GetHashCode(field), comparer.GetHashCode(field));
    // }

    // [Fact]
    // public void GetHashCode_DifferentField_DifferentHashCode()
    // {
    //     var comparer = new FieldEqualityComparer();
    //     Assert.NotEqual(comparer.GetHashCode(new Field("field", new Int32DataType())), comparer.GetHashCode(new Field("field", new Int64DataType())));
    // }
}