using System.Collections;

namespace DeltaLake.Protocol;

public sealed record DeltaArray<T> : IEquatable<DeltaArray<T>>, IList<T>
    where T : IEquatable<T>
{
    private readonly List<T> _list;

    public DeltaArray() => _list = [];
    public DeltaArray(List<T> list) => _list = list;
    public DeltaArray(IEnumerable<T> list) => _list = new List<T>(list);

    public T this[int index] { get => ((IList<T>)_list)[index]; set => ((IList<T>)_list)[index] = value; }
    public int Count => ((ICollection<T>)_list).Count;
    public bool IsReadOnly => ((ICollection<T>)_list).IsReadOnly;
    public void Add(T item) => ((ICollection<T>)_list).Add(item);
    public void Clear() => ((ICollection<T>)_list).Clear();
    public bool Contains(T item) => ((ICollection<T>)_list).Contains(item);
    public void CopyTo(T[] array, int arrayIndex) => ((ICollection<T>)_list).CopyTo(array, arrayIndex);
    public bool Equals(DeltaArray<T>? other) => _list.SequenceEqual(other?._list ?? []);
    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var item in _list)
        {
            hash.Add(item);
        }
        return hash.ToHashCode();
    }
    public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)_list).GetEnumerator();
    public int IndexOf(T item) => ((IList<T>)_list).IndexOf(item);
    public void Insert(int index, T item) => ((IList<T>)_list).Insert(index, item);
    public bool Remove(T item) => ((ICollection<T>)_list).Remove(item);
    public void RemoveAt(int index) => ((IList<T>)_list).RemoveAt(index);
    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_list).GetEnumerator();

}
