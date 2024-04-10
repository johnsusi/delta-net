using System.Diagnostics.CodeAnalysis;

namespace Apache.Arrow.Types;

public sealed class ArrowTypeEqualityComparer : IEqualityComparer<IArrowType>
{
    public bool Equals(IArrowType? x, IArrowType? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        if (x.TypeId != y.TypeId) return false;
        if (x.Name != y.Name) return false;
        if (x.IsFixedWidth != y.IsFixedWidth) return false;
        return true;
    }

    public int GetHashCode([DisallowNull] IArrowType obj)
    {
        return HashCode.Combine(obj.TypeId, obj.Name, obj.IsFixedWidth);
    }

}
