using Apache.Arrow;

namespace DeltaLake;

[AttributeUsage(AttributeTargets.Struct | AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
public class TestAttribute : Attribute
{
}

public interface ITable
{
    public static abstract Schema Schema { get; }

}


public interface ITable<T> : ITable
{
    public static abstract IEnumerable<T> Enumerate(RecordBatch batch);
}
