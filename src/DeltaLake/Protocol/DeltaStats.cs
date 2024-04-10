using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using ParquetSharp;

namespace DeltaLake.Protocol;

public record class DeltaStats
{
    public long NumRecords { get; private set; }
    public DeltaMap<string, object> MinValues { get; }
    public DeltaMap<string, object> MaxValues { get; }
    public DeltaMap<string, long> NullCounts { get; }

    public DeltaStats(long numRecords = 0, DeltaMap<string, object>? minValues = null, DeltaMap<string, object>? maxValues = null, DeltaMap<string, long>? nullCounts = null)
    {
        NumRecords = numRecords;
        MinValues = minValues ?? [];
        MaxValues = maxValues ?? [];
        NullCounts = nullCounts ?? [];
    }

    public void Update(RecordBatch batch)
    {
        NumRecords += batch.Length;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            var name = batch.Schema.GetFieldByIndex(i).Name;
            if (NullCounts.TryGetValue(name, out var count))
                NullCounts[name] += column.NullCount;
            else
                NullCounts[name] = column.NullCount;
            MinValues.TryGetValue(name, out var min);
            MaxValues.TryGetValue(name, out var max);
            var visitor = new DeltaStatsVisitor(min, max);
            column.Accept(visitor);
            if (visitor.Min is not null)
                MinValues[name] = visitor.Min;
            if (visitor.Max is not null)
                MaxValues[name] = visitor.Max;
        }
    }


    internal class DeltaStatsVisitor :
                IArrowArrayVisitor<Int8Array>,
                IArrowArrayVisitor<Int16Array>,
                IArrowArrayVisitor<Int32Array>,
                IArrowArrayVisitor<Int64Array>,
                IArrowArrayVisitor<UInt8Array>,
                IArrowArrayVisitor<UInt16Array>,
                IArrowArrayVisitor<UInt32Array>,
                IArrowArrayVisitor<UInt64Array>,
                IArrowArrayVisitor<HalfFloatArray>,
                IArrowArrayVisitor<FloatArray>,
                IArrowArrayVisitor<DoubleArray>,
                IArrowArrayVisitor<BooleanArray>,
                IArrowArrayVisitor<TimestampArray>,
                IArrowArrayVisitor<Date32Array>,
                IArrowArrayVisitor<Date64Array>,
                IArrowArrayVisitor<Time32Array>,
                IArrowArrayVisitor<Time64Array>,
                IArrowArrayVisitor<DurationArray>,
                IArrowArrayVisitor<YearMonthIntervalArray>,
                IArrowArrayVisitor<DayTimeIntervalArray>,
                IArrowArrayVisitor<MonthDayNanosecondIntervalArray>,
                IArrowArrayVisitor<ListArray>,
                IArrowArrayVisitor<ListViewArray>,
                IArrowArrayVisitor<FixedSizeListArray>,
                IArrowArrayVisitor<StringArray>,
                IArrowArrayVisitor<StringViewArray>,
                IArrowArrayVisitor<BinaryArray>,
                IArrowArrayVisitor<BinaryViewArray>,
                IArrowArrayVisitor<FixedSizeBinaryArray>,
                IArrowArrayVisitor<StructArray>,
                IArrowArrayVisitor<UnionArray>,
                IArrowArrayVisitor<Decimal128Array>,
                IArrowArrayVisitor<Decimal256Array>,
                IArrowArrayVisitor<DictionaryArray>,
                IArrowArrayVisitor<NullArray>
    {

        public object? Min { get; private set; }
        public object? Max { get; private set; }

        public DeltaStatsVisitor(object? min, object? max)
        {
            Min = min;
            Max = max;
        }

        public void FindMinMax<T>(IReadOnlyList<T> array)
        // where T : struct, INumber<T>
        {
            var comparer = Comparer<T>.Default;
            foreach (var item in array)
            {
                if (item is null) continue;
                Min = Min is null ? item : comparer.Compare((T)Min, item) < 0 ? Min : item;
                Max = Max is null ? item : comparer.Compare((T)Max, item) > 0 ? Max : item;
            }
            // for (int i = 0; i < array.Count; i++)
            // {
            //     if (array.IsNull(i)) continue;
            //     var value = array.GetValue(i);
            //     if (value.HasValue)
            //     {
            //         Min = Min is null ? value.Value : T.Min((T)Min, value.Value);
            //         Max = Max is null ? value.Value : T.Max((T)Max, value.Value);
            //     }
            // }
        }

        public void Visit(Int8Array array) => FindMinMax(array);
        public void Visit(Int16Array array) => FindMinMax(array);
        public void Visit(Int32Array array) => FindMinMax(array);
        public void Visit(Int64Array array) => FindMinMax(array);
        public void Visit(UInt8Array array) => FindMinMax(array);
        public void Visit(UInt16Array array) => FindMinMax(array);
        public void Visit(UInt32Array array) => FindMinMax(array);
        public void Visit(UInt64Array array) => FindMinMax(array);
        public void Visit(HalfFloatArray array) => FindMinMax(array);
        public void Visit(FloatArray array) => FindMinMax(array);
        public void Visit(DoubleArray array) => FindMinMax(array);
        public void Visit(BooleanArray array) => FindMinMax(array);
        public void Visit(TimestampArray array) => FindMinMax(array as IReadOnlyList<DateTimeOffset?>);
        public void Visit(Date32Array array) => FindMinMax(array as IReadOnlyList<DateTime?>);
        public void Visit(Date64Array array) => FindMinMax(array as IReadOnlyList<DateTime?>);
        public void Visit(Time32Array array) => FindMinMax(array as IReadOnlyList<TimeOnly?>);
        public void Visit(Time64Array array) => FindMinMax(array as IReadOnlyList<TimeOnly?>);
        public void Visit(DurationArray array) => FindMinMax(array as IReadOnlyList<TimeSpan?>);
        public void Visit(YearMonthIntervalArray array) => FindMinMax(array);
        public void Visit(DayTimeIntervalArray array) => FindMinMax(array);
        public void Visit(MonthDayNanosecondIntervalArray array) => FindMinMax(array);
        public void Visit(ListArray array) => throw new NotImplementedException("ListArray");
        public void Visit(ListViewArray array) => throw new NotImplementedException("ListViewArray");
        public void Visit(FixedSizeListArray array) => throw new NotImplementedException("FixedSizeListArray");
        public void Visit(StringArray array) => FindMinMax(array as IReadOnlyList<string>);
        public void Visit(StringViewArray array) => FindMinMax(array as IReadOnlyList<string>);
        public void Visit(BinaryArray array) => throw new NotImplementedException("BinaryArray");
        public void Visit(BinaryViewArray array) => throw new NotImplementedException("BinaryViewArray");
        public void Visit(FixedSizeBinaryArray array) => throw new NotImplementedException("FixedSizeBinaryArray");
        public void Visit(StructArray array) => throw new NotImplementedException("StructArray");
        public void Visit(UnionArray array) => throw new NotImplementedException("UnionArray");
        public void Visit(Decimal128Array array) => throw new NotImplementedException("Decimal128Array");
        public void Visit(Decimal256Array array) => throw new NotImplementedException("Decimal256Array");
        public void Visit(DictionaryArray array) => throw new NotImplementedException("DictionaryArray");
        public void Visit(NullArray array) => throw new NotImplementedException("NullArray");
        public void Visit(IArrowArray array) => throw new NotImplementedException("IArrowArray");
    }

}
