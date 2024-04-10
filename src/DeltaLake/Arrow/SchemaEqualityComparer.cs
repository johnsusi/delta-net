using System.Diagnostics.CodeAnalysis;

namespace Apache.Arrow;

public class SchemaEqualityComparer : IEqualityComparer<Schema>
{

    private static readonly FieldEqualityComparer FieldEqualityComparer = new();
    public bool Equals(Schema? x, Schema? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        if (x.FieldsList.Count != y.FieldsList.Count) return false;
        if (x.FieldsList.Except(y.FieldsList, FieldEqualityComparer).Any()) return false;
        return true;

    }

    public int GetHashCode([DisallowNull] Schema obj)
    {
        var hashCode = new HashCode();
        foreach (var field in obj.FieldsList)
        {
            hashCode.Add(FieldEqualityComparer.GetHashCode(field));
        }
        return hashCode.ToHashCode();
    }
}
