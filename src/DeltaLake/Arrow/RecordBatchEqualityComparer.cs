using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Apache.Arrow.Arrays;

namespace Apache.Arrow;


public sealed class RecordBatchEqualityComparer : IEqualityComparer<RecordBatch>
{
    public bool Equals(RecordBatch? x, RecordBatch? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        if (x.ColumnCount != y.ColumnCount) return false;
        if (x.Length != y.Length) return false;
        if (!new SchemaEqualityComparer().Equals(x.Schema, y.Schema)) return false;

        for (var i = 0; i < x.ColumnCount; i++)
        {
            var xColumn = x.Column(i);
            var yColumn = y.Column(i);
            switch ((xColumn, yColumn))
            {
                case (null, null):
                    continue;
                case (null, not null):
                case (not null, null):
                    return false;
                case (var first, var second):
                    if (xColumn.GetType() != yColumn.GetType()) return false;
                    var visitor = new ArrayVisitor { Other = second };
                    first.Accept(visitor);
                    var result = visitor.Result ?? false;
                    if (!result) return false;
                    continue;
            }
        }
        return true;
    }

    public int GetHashCode(RecordBatch obj) => obj.GetHashCode();

    internal class ArrayVisitor :
        IArrowArrayVisitor<BinaryArray>,
        IArrowArrayVisitor<BinaryViewArray>,
        IArrowArrayVisitor<BooleanArray>,
        IArrowArrayVisitor<Date32Array>,
        IArrowArrayVisitor<Date64Array>,
        // IArrowArrayVisitor<DenseUnionArray>,
        // IArrowArrayVisitor<DictionaryArray>,
        IArrowArrayVisitor<Decimal128Array>,
        IArrowArrayVisitor<Decimal256Array>,
        IArrowArrayVisitor<DoubleArray>,
        IArrowArrayVisitor<DurationArray>,
        // IArrowArrayVisitor<FixedSizeListArray>,
        IArrowArrayVisitor<FloatArray>,
        IArrowArrayVisitor<HalfFloatArray>,
        IArrowArrayVisitor<Int16Array>,
        IArrowArrayVisitor<Int32Array>,
        IArrowArrayVisitor<Int64Array>,
        IArrowArrayVisitor<Int8Array>,
        IArrowArrayVisitor<YearMonthIntervalArray>,
        IArrowArrayVisitor<DayTimeIntervalArray>,
        IArrowArrayVisitor<MonthDayNanosecondIntervalArray>,
        // IArrowArrayVisitor<ListArray>,
        // IArrowArrayVisitor<ListViewArray>,
        // IArrowArrayVisitor<MapArray>,
        IArrowArrayVisitor<NullArray>,
        // IArrowArrayVisitor<SparseUnionArray>,
        // IArrowArrayVisitor<StructArray>,
        IArrowArrayVisitor<StringArray>,
        IArrowArrayVisitor<StringViewArray>,
        IArrowArrayVisitor<Time32Array>,
        IArrowArrayVisitor<Time64Array>,
        IArrowArrayVisitor<TimestampArray>,
        IArrowArrayVisitor<UInt16Array>,
        IArrowArrayVisitor<UInt32Array>,
        IArrowArrayVisitor<UInt64Array>,
        IArrowArrayVisitor<UInt8Array>,
        // IArrowArrayVisitor<RecordBatch>,
        IArrowArrayVisitor<FixedSizeBinaryArray>
    {
        public object? Other { get; init; }
        public bool? Result { get; private set; }

        public void Visit(IArrowArray array)
        {
            throw new NotImplementedException();
        }

        public void Visit(BinaryArray array)
            => Result = IsEqual(array, Other as BinaryArray);
        public void Visit(BinaryViewArray array)
            => Result = IsEqual(array, Other as BinaryViewArray);
        public void Visit(BooleanArray array)
            => Result = IsEqual(array, Other as BooleanArray);

        public void Visit(Date32Array array)
            => Result = IsEqual(array, Other as Date32Array);

        public void Visit(Date64Array array)
            => Result = IsEqual(array, Other as Date64Array);

        public void Visit(Decimal128Array array)
            => Visit(array as FixedSizeBinaryArray);

        public void Visit(Decimal256Array array)
            => Visit(array as FixedSizeBinaryArray);

        // public void Visit(DenseUnionArray array)
        //     => Result = IsEqual(array, Other as DenseUnionArray);

        // public void Visit(DictionaryArray array)
        //     => Result = IsEqual(array, Other as DictionaryArray);

        public void Visit(DoubleArray array)
            => Result = IsEqual(array, Other as DoubleArray);

        public void Visit(DurationArray array)
            => Result = IsEqual(array, Other as DurationArray);

        // public void Visit(FixedSizeListArray array)
        //     => Result = IsEqual(array, Other as FixedSizeListArray);

        public void Visit(FloatArray array)
            => Result = IsEqual(array, Other as FloatArray);

        public void Visit(HalfFloatArray array)
            => Result = IsEqual(array, Other as HalfFloatArray);

        public void Visit(Int16Array array)
            => Result = IsEqual(array, Other as Int16Array);

        public void Visit(Int32Array array)
            => Result = IsEqual(array, Other as Int32Array);

        public void Visit(Int64Array array)
            => Result = IsEqual(array, Other as Int64Array);

        public void Visit(Int8Array array)
            => Result = IsEqual(array, Other as Int8Array);

        public void Visit(YearMonthIntervalArray array)
            => Result = IsEqual(array, Other as YearMonthIntervalArray);

        public void Visit(DayTimeIntervalArray array)
            => Result = IsEqual(array, Other as DayTimeIntervalArray);

        public void Visit(MonthDayNanosecondIntervalArray array)
            => Result = IsEqual(array, Other as MonthDayNanosecondIntervalArray);

        // public void Visit(ListArray array)
        //     => Result = IsEqual(array, Other as ListArray);

        // public void Visit(ListViewArray array)
        //     => Result = IsEqual(array, Other as ListViewArray);

        // public void Visit(MapArray array)
        //     => Result = IsEqual(array, Other as MapArray);

        public void Visit(NullArray array)
            => Result = IsEqual(array, Other as NullArray);

        // public void Visit(SparseUnionArray array)
        //     => Result = IsEqual(array, Other as SparseUnionArray);

        public void Visit(StringArray array)
            => Result = IsEqual<string>(array, Other as StringArray);

        public void Visit(StringViewArray array)
            => Result = IsEqual<string>(array, Other as StringViewArray);

        // public void Visit(StructArray array)
        //     => Result = IsEqual(array, Other as StructArray);

        public void Visit(Time32Array array)
            => Result = IsEqual(array, Other as Time32Array);

        public void Visit(Time64Array array)
            => Result = IsEqual(array, Other as Time64Array);

        public void Visit(TimestampArray array)
            => Result = IsEqual(array, Other as TimestampArray);

        public void Visit(UInt16Array array)
            => Result = IsEqual(array, Other as UInt16Array);

        public void Visit(UInt32Array array)
            => Result = IsEqual(array, Other as UInt32Array);

        public void Visit(UInt64Array array)
            => Result = IsEqual(array, Other as UInt64Array);

        public void Visit(UInt8Array array)
            => Result = IsEqual(array, Other as UInt8Array);

        // public void Visit(RecordBatch array)
        //     => Result = IsEqual(array, Other as RecordBatch);

        public void Visit(FixedSizeBinaryArray array)
            => Result = IsEqual(array, Other as FixedSizeBinaryArray);


        public static bool IsEqual(NullArray first, NullArray? second)
            => second is not null && first.Length == second.Length && first.NullCount == second.NullCount;

        public static bool IsEqual<T>(PrimitiveArray<T> first, PrimitiveArray<T>? second)
            where T : struct
            => second is not null && IsEqual(first as IReadOnlyList<T?>, second as IReadOnlyList<T?>);

        public static bool IsEqual<T>(IReadOnlyList<T> first, IReadOnlyList<T>? second)
            => second is not null && first.SequenceEqual(second, EqualityComparer<T>.Create(
                (T? x, T? y) => (x, y) switch
                {
                    (null, null) => true,
                    (null, not null) => false,
                    (not null, null) => false,
                    (byte[] x2, byte[] y2) => x2.SequenceEqual(y2),
                    (var x2, var y2) => EqualityComparer<T>.Default.Equals(x2, y2)
                },
                (T obj) => EqualityComparer<T>.Default.GetHashCode(obj!)
            ));

    }
}
