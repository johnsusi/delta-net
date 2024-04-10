using System.Collections;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaArrayTests
{
    [Fact]
    public void New_FromEnumerable_ShouldCreate()
    {
        static IEnumerable<int> Enumerate()
        {
            yield return 1;
        }
        var deltaArray = new DeltaArray<int>(Enumerate());

        Assert.NotNull(deltaArray);
    }

    [Fact]
    public void New_FromList_ShouldCreate()
    {
        var deltaArray = new DeltaArray<int>(new List<int>());

        Assert.NotNull(deltaArray);
    }

    [Fact]
    public void Count_ShouldReturnCount()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var count = deltaArray.Count;

        Assert.Equal(3, count);
    }

    [Fact]
    public void IsReadOnly_ShouldReturnFalse()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var isReadOnly = deltaArray.IsReadOnly;

        Assert.False(isReadOnly);
    }

    [Fact]
    public void Add_ShouldAddItem()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray.Add(4);

        Assert.Equal(4, deltaArray.Count);
    }

    [Fact]
    public void Clear_ShouldClearItems()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray.Clear();

        Assert.Empty(deltaArray);
    }

    [Fact]
    public void Contains_ShouldReturnTrue()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var contains = deltaArray.Contains(2);

        Assert.True(contains);
    }

    [Fact]
    public void Contains_ShouldReturnFalse()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var contains = deltaArray.Contains(4);

        Assert.False(contains);
    }

    [Fact]
    public void CopyTo_ShouldCopyItems()
    {
        int[] expected = [1, 2, 3];
        DeltaArray<int> deltaArray = [1, 2, 3];
        var actual = new int[3];

        deltaArray.CopyTo(actual, 0);

        Assert.Equal(expected, actual);
    }


    [Fact]
    public void GetEnumerator_ShouldReturnEnumerator()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var enumerator = deltaArray.GetEnumerator();

        Assert.NotNull(enumerator);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(1, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(2, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(3, enumerator.Current);
        Assert.False(enumerator.MoveNext());

    }


    [Fact]
    public void IEnumerable_GetEnumerator_ShouldEnumerateItems()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        IEnumerable enumerable = deltaArray;

        var enumerator = enumerable.GetEnumerator();

        Assert.NotNull(enumerator);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(1, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(2, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(3, enumerator.Current);
        Assert.False(enumerator.MoveNext());
    }

    [Fact]
    public void Equals_ShouldReturnTrue()
    {
        DeltaArray<int> deltaArray1 = [1, 2, 3];
        DeltaArray<int> deltaArray2 = [1, 2, 3];

        var equals = deltaArray1.Equals(deltaArray2);

        Assert.True(equals);
    }

    [Fact]
    public void Equals_ShouldReturnFalse()
    {
        DeltaArray<int> deltaArray1 = [1, 2, 3];
        DeltaArray<int> deltaArray2 = [1, 2, 4];

        var equals = deltaArray1.Equals(deltaArray2);

        Assert.False(equals);
    }

    [Fact]
    public void Equals_WithNull_ShouldReturnFalse()
    {
        DeltaArray<int> deltaArray1 = [1, 2, 3];
        DeltaArray<int>? deltaArray2 = null;

        var equals = deltaArray1.Equals(deltaArray2);

        Assert.False(equals);
    }

    [Fact]
    public void GetHashCode_ShouldReturnSame()
    {
        DeltaArray<int> deltaArray1 = [1, 2, 3];
        DeltaArray<int> deltaArray2 = [1, 2, 3];

        var equals = deltaArray1.GetHashCode().Equals(deltaArray2.GetHashCode());

        Assert.True(equals);
    }

    [Fact]
    public void IndexOf_ShouldReturnIndex()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var index = deltaArray.IndexOf(2);

        Assert.Equal(1, index);
    }

    [Fact]
    public void Insert_ShouldInsertItemAtIndex()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray.Insert(1, 4);

        Assert.Equal([1, 4, 2, 3], deltaArray);
    }

    [Fact]
    public void Remove_ShouldRemoveItem()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray.Remove(2);

        Assert.Equal([1, 3], deltaArray);
    }

    [Fact]
    public void RemoveAt_ShouldRemoveItemAtIndex()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray.RemoveAt(1);

        Assert.Equal([1, 3], deltaArray);
    }

    [Fact]
    public void Indexer_ShouldGetItem()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        var item = deltaArray[1];

        Assert.Equal(2, item);
    }

    [Fact]
    public void Indexer_ShouldSetItem()
    {
        DeltaArray<int> deltaArray = [1, 2, 3];

        deltaArray[1] = 4;

        Assert.Equal([1, 4, 3], deltaArray);
    }

}
