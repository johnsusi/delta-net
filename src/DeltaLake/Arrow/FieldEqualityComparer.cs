using System.Diagnostics.CodeAnalysis;
using Apache.Arrow.Types;

namespace Apache.Arrow;

public sealed class FieldEqualityComparer : IEqualityComparer<Field>
{
    private static readonly ArrowTypeEqualityComparer ArrowTypeEqualityComparer = new();

    public bool Equals(Field? x, Field? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        if (x.Name != y.Name) return false;
        if (x.IsNullable != y.IsNullable) return false;
        if (x.HasMetadata)
        {
            if (!y.HasMetadata) return false;
            if (!new MetadataEqualityComparer().Equals(x.Metadata, y.Metadata)) return false;
        }
        return ArrowTypeEqualityComparer.Equals(x.DataType, y.DataType);
    }

    public int GetHashCode([DisallowNull] Field obj)
    {
        var hashCode = new HashCode();
        hashCode.Add(obj.Name);
        hashCode.Add(obj.IsNullable);
        if (obj.HasMetadata)
            hashCode.Add(obj.Metadata, new MetadataEqualityComparer());

        hashCode.Add(obj.DataType, ArrowTypeEqualityComparer);
        return hashCode.ToHashCode();
    }

    internal sealed class MetadataEqualityComparer : IEqualityComparer<IReadOnlyDictionary<string, string>>
    {

        public bool Equals(IReadOnlyDictionary<string, string>? x, IReadOnlyDictionary<string, string>? y)
        {
            if (x is null && y is null) return true;
            if (x is null || y is null) return false;
            if (x.Count != y.Count) return false;
            return !x.Except(y).Any();
        }

        public int GetHashCode([DisallowNull] IReadOnlyDictionary<string, string> obj)
        {
            var hashCode = new HashCode();
            foreach (var pair in obj)
            {
                hashCode.Add(pair);
            }
            return hashCode.ToHashCode();
        }
    }
}

