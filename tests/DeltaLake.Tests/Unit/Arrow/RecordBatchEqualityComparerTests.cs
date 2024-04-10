using Apache.Arrow;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace DeltaLake.Tests.Unit.Arrow;

public class RecordBatchEqualityComparerTests
{

    public static IEnumerable<object?[]> ValidTestCases()
    {
        var schema1 = new Schema([
            new Field("null", NullType.Default, false),
            new Field("bool", BooleanType.Default, false),
            new Field("int8", Int8Type.Default, false),
            new Field("int16", Int16Type.Default, false),
            new Field("int32", Int32Type.Default, false),
            new Field("int64", Int64Type.Default, false),
            new Field("uint8", UInt8Type.Default, false),
            new Field("uint16", UInt16Type.Default, false),
            new Field("uint32", UInt32Type.Default, false),
            new Field("uint64", UInt64Type.Default, false),
            new Field("half", HalfFloatType.Default, false),
            new Field("float", FloatType.Default, false),
            new Field("double", DoubleType.Default, false),
            new Field("dec128", new Decimal128Type(10, 6), false),
            new Field("dec256", new Decimal256Type(16, 8), false),
            new Field("string", StringType.Default, false),
            new Field("stringview", StringViewType.Default, false),
            new Field("date32", Date32Type.Default, false),
            new Field("date64", Date64Type.Default, false),
            new Field("time32", Time32Type.Default, false),
            new Field("time64", Time64Type.Default, false),
            new Field("ds", DurationType.Second, false),
            new Field("dms", DurationType.Millisecond, false),
            new Field("dus", DurationType.Microsecond, false),
            new Field("dns", DurationType.Nanosecond, false),
            new Field("iym", IntervalType.YearMonth, false),
            new Field("idt", IntervalType.DayTime, false),
            new Field("imdn", IntervalType.MonthDayNanosecond, false),
            new Field("tsmilli", new TimestampType(TimeUnit.Millisecond, TimeZoneInfo.Utc), false),
            new Field("tsmicro", new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc), false),
            new Field("tsnano", new TimestampType(TimeUnit.Nanosecond, TimeZoneInfo.Utc), false),
            new Field("bin", BinaryType.Default, false),
            new Field("binview", BinaryViewType.Default, false),
        ], []);
        var batch1 = new RecordBatch(schema1, [
            new NullArray.Builder().AppendNull().Build(),
            new BooleanArray.Builder().Append(true).Build(),
            new Int8Array.Builder().Append(1).Build(),
            new Int16Array.Builder().Append(1).Build(),
            new Int32Array.Builder().Append(1).Build(),
            new Int64Array.Builder().Append(1).Build(),
            new UInt8Array.Builder().Append(1).Build(),
            new UInt16Array.Builder().Append(1).Build(),
            new UInt32Array.Builder().Append(1).Build(),
            new UInt64Array.Builder().Append(1).Build(),
            new HalfFloatArray.Builder().Append((Half)1).Build(),
            new FloatArray.Builder().Append(1).Build(),
            new DoubleArray.Builder().Append(1).Build(),
            new Decimal128Array.Builder(new Decimal128Type(10, 6)).Append(1).Build(),
            new Decimal256Array.Builder(new Decimal256Type(16, 8)).Append(1).Build(),
            new StringArray.Builder().Append("a").Build(),
            new StringViewArray.Builder().Append("a").Build(),
            new Date32Array.Builder().Append(DateTime.UtcNow).Build(),
            new Date64Array.Builder().Append(DateTime.UtcNow).Build(),
            new Time32Array.Builder().Append(1).Build(),
            new Time64Array.Builder().Append(1).Build(),
            new DurationArray.Builder(DurationType.Second).Append(TimeSpan.FromSeconds(1)).Build(),
            new DurationArray.Builder(DurationType.Millisecond).Append(TimeSpan.FromSeconds(1)).Build(),
            new DurationArray.Builder(DurationType.Microsecond).Append(TimeSpan.FromSeconds(1)).Build(),
            new DurationArray.Builder(DurationType.Nanosecond).Append(TimeSpan.FromSeconds(1)).Build(),
            new YearMonthIntervalArray.Builder().Append(new YearMonthInterval(1, 1)).Build(),
            new DayTimeIntervalArray.Builder().Append(new DayTimeInterval(1, 1)).Build(),
            new MonthDayNanosecondIntervalArray.Builder().Append(new MonthDayNanosecondInterval(1, 1, 1)).Build(),
            new TimestampArray.Builder().Append(DateTime.UtcNow).Build(),
            new TimestampArray.Builder().Append(DateTime.UtcNow).Build(),
            new TimestampArray.Builder().Append(DateTime.UtcNow).Build(),
            new BinaryArray.Builder().Append([ 1 ]).Build(),
            new BinaryViewArray.Builder().Append([ 1 ]).Build(),
        ], 1);

        yield return [
            null,
            null
        ];

        yield return [
            batch1,
            batch1
        ];

        yield return [
            batch1,
            batch1.Clone()
        ];
    }

    public static IEnumerable<object?[]> InvalidTestCases()
    {
        var schema1 = new Schema([
            new Field("foo", Int32Type.Default, false)
        ], []);
        var schema2 = new Schema([
            new Field("foo", Int32Type.Default, true)
        ], []);
        var schema3 = schema1.InsertField(1, new Field("bar", StringType.Default, false));
        RecordBatch[] batches = [
            new (schema1, [new Int32Array.Builder().Append(1).Build()], 1),
            new (schema1, [new Int32Array.Builder().Append(1).Append(2).Build()], 2),
            new (schema2, [new Int32Array.Builder().Append(1).Build()], 1),
            new (schema1, [new Int32Array.Builder().Append(2).Build()], 1),
            new (schema3, [new Int32Array.Builder().Append(1).Build(), new StringArray.Builder().Append("a").Build()], 1)
        ];

        yield return [
            null,
            batches[0]
        ];

        yield return [
            batches[0],
            null
        ];

        for (var i = 0; i < batches.Length; i++)
        {
            for (var j = 0; j < batches.Length; j++)
            {
                if (i == j) continue;
                yield return [
                    batches[i],
                    batches[j]
                ];
            }
        }

        yield return [
            new RecordBatch(schema1, [new Int32Array.Builder().Append(1).Build()], 1),
            new RecordBatch(schema1, [new Int16Array.Builder().Append(1).Build()], 1)
        ];

        yield return [
            new RecordBatch(schema1, [], 1),
            new RecordBatch(schema1, [new Int32Array.Builder().Append(1).Build()], 1),
        ];

        yield return [
            new RecordBatch(schema1, [new Int32Array.Builder().Append(1).Build()], 1),
            new RecordBatch(schema1, [], 1)
        ];


    }

    [Theory]
    [MemberData(nameof(ValidTestCases))]
    public void Equal_WithEqualRecordBatches_ShouldReturnTrue(RecordBatch recordBatchX, RecordBatch recordBatchY)
    {
        var comparer = new RecordBatchEqualityComparer();
        var actual = comparer.Equals(recordBatchX, recordBatchY);
        Assert.True(actual);
    }

    [Theory]
    [MemberData(nameof(InvalidTestCases))]
    public void Equal_WithUnequalRecordBatches_ShouldReturnFalse(RecordBatch recordBatchX, RecordBatch recordBatchY)
    {
        var comparer = new RecordBatchEqualityComparer();
        var actual = comparer.Equals(recordBatchX, recordBatchY);
        Assert.False(actual);
    }

}
