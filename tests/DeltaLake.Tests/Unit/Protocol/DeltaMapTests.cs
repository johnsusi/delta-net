using System.Collections;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaMapTests
{

    [Fact]
    public void New_FromDictionary_ShouldCreate()
    {
        var deltaMap = new DeltaMap<int, string>([]);

        Assert.NotNull(deltaMap);
    }

    [Fact]
    public void New_FromEnumerable_ShouldCreate()
    {
        static IEnumerable<KeyValuePair<int, string>> Enumerate()
        {
            yield return new(1, "one");
        }
        var deltaMap = new DeltaMap<int, string>(Enumerate());

        Assert.NotNull(deltaMap);
    }

    [Fact]
    public void Count_ShouldReturnCount()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var count = deltaMap.Count;

        Assert.Equal(1, count);
    }

    [Fact]
    public void IsReadOnly_ShouldReturnFalse()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var isReadOnly = deltaMap.IsReadOnly;

        Assert.False(isReadOnly);
    }

    [Fact]
    public void Add_ShouldAddItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        deltaMap.Add(2, "two");

        Assert.Equal(2, deltaMap.Count);
    }

    [Fact]
    public void Clear_ShouldClearItems()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        deltaMap.Clear();

        Assert.Empty(deltaMap);
    }

    [Fact]
    public void ContainsKey_ShouldReturnTrue()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var containsKey = deltaMap.ContainsKey(1);

        Assert.True(containsKey);
    }

    [Fact]
    public void ContainsKey_ShouldReturnFalse()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var containsKey = deltaMap.ContainsKey(2);

        Assert.False(containsKey);
    }

    [Fact]
    public void Contains_ShouldReturnTrue()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var contains = deltaMap.Contains(new(1, "one"));

        Assert.True(contains);
    }

    [Fact]
    public void Contains_ShouldReturnFalse()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var contains = deltaMap.Contains(new(2, "two"));

        Assert.False(contains);
    }

    [Fact]
    public void CopyTo_ShouldCopyItems()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];
        var array = new KeyValuePair<int, string>[1];

        deltaMap.CopyTo(array, 0);

        Assert.Equal(new KeyValuePair<int, string>(1, "one"), array[0]);
    }

    [Fact]
    public void GetEnumerator_ShouldEnumerateItems()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var enumerator = deltaMap.GetEnumerator();

        Assert.NotNull(enumerator);
    }

    [Fact]
    public void IEnumerable_GetEnumerator_ShouldEnumerateItems()
    {
        IEnumerable enumerable = new DeltaMap<int, string>();

        var enumerator = enumerable.GetEnumerator();

        Assert.NotNull(enumerator);
    }

    [Fact]
    public void Remove_ShouldRemoveItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var removed = deltaMap.Remove(1);

        Assert.True(removed);
    }

    [Fact]
    public void Remove_WithMatchingKeyValuePair_ShouldRemoveItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var removed = deltaMap.Remove(new KeyValuePair<int, string>(1, "one"));

        Assert.True(removed);
    }

    [Fact]
    public void Remove_ShouldNotRemoveItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var removed = deltaMap.Remove(2);

        Assert.False(removed);
    }

    [Fact]
    public void Remove_WithOtherKeyValuePair_ShouldNotRemoveItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var removed = deltaMap.Remove(new KeyValuePair<int, string>(2, "one"));

        Assert.False(removed);
    }

    [Fact]
    public void TryGetValue_ShouldReturnTrue()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var tryGetValue = deltaMap.TryGetValue(1, out var value);

        Assert.True(tryGetValue);
        Assert.Equal("one", value);
    }

    [Fact]
    public void TryGetValue_ShouldReturnFalse()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var tryGetValue = deltaMap.TryGetValue(2, out var value);

        Assert.False(tryGetValue);
        Assert.Null(value);
    }

    [Fact]
    public void Indexer_ShouldGetAndSetItem()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var value = deltaMap[1];

        Assert.Equal("one", value);

        deltaMap[1] = "two";

        Assert.Equal("two", deltaMap[1]);
    }

    [Fact]
    public void Keys_ShouldReturnKeys()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var keys = deltaMap.Keys;

        Assert.Equal(new[] { 1 }, keys);
    }

    [Fact]
    public void Values_ShouldReturnValues()
    {
        DeltaMap<int, string> deltaMap = [new(1, "one")];

        var values = deltaMap.Values;

        Assert.Equal(new[] { "one" }, values);
    }

    [Fact]
    public void Equals_ShouldReturnTrue()
    {
        DeltaMap<int, string> deltaMap1 = [new(1, "one")];
        DeltaMap<int, string> deltaMap2 = [new(1, "one")];

        var equals = deltaMap1.Equals(deltaMap2);

        Assert.True(equals);
    }

    [Fact]
    public void Equals_ShouldReturnFalse()
    {
        DeltaMap<int, string> deltaMap1 = [new(1, "one")];
        DeltaMap<int, string> deltaMap2 = [new(1, "two")];

        var equals = deltaMap1.Equals(deltaMap2);

        Assert.False(equals);
    }

    [Fact]
    public void Equals_WithSecondNull_ReturnsFalse()
    {
        DeltaMap<int, string> deltaMap1 = [new(1, "one")];
        DeltaMap<int, string>? deltaMap2 = null;

        var equals = deltaMap1.Equals(deltaMap2);

        Assert.False(equals);
    }

    [Fact]
    public void GetHashCode_ShouldReturnSame()
    {
        DeltaMap<int, string> deltaMap1 = [new(1, "one")];
        DeltaMap<int, string> deltaMap2 = [new(1, "one")];

        var equals = deltaMap1.GetHashCode().Equals(deltaMap2.GetHashCode());

        Assert.True(equals);
    }

}
