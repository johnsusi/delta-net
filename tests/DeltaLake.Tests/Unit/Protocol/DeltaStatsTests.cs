using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Protocol;

namespace DeltaLake.Tests.Unit.Protocol;

public class DeltaStatsTests
{

    public static IEnumerable<object[]> TestCases()
    {
        yield return [
            BooleanType.Default, false, true, new BooleanArray.Builder().Append(false).AppendNull().Append(true).Build(),
        ];
        yield return [
            Int8Type.Default, sbyte.MinValue, sbyte.MaxValue, new Int8Array.Builder().Append(sbyte.MinValue).AppendNull().Append(sbyte.MaxValue).Build(),
        ];
        yield return [
            Int16Type.Default, short.MinValue, short.MaxValue, new Int16Array.Builder().Append(short.MinValue).AppendNull().Append(short.MaxValue).Build(),
        ];
        yield return [
            Int32Type.Default, int.MinValue, int.MaxValue, new Int32Array.Builder().Append(int.MinValue).AppendNull().Append(int.MaxValue).Build(),
        ];
        yield return [
            Int64Type.Default, long.MinValue, long.MaxValue, new Int64Array.Builder().Append(long.MinValue).AppendNull().Append(long.MaxValue).Build(),
        ];
        yield return [
            UInt8Type.Default, byte.MinValue, byte.MaxValue, new UInt8Array.Builder().Append(byte.MinValue).AppendNull().Append(byte.MaxValue).Build(),
        ];
        yield return [
            UInt16Type.Default, ushort.MinValue, ushort.MaxValue, new UInt16Array.Builder().Append(ushort.MinValue).AppendNull().Append(ushort.MaxValue).Build(),
        ];
        yield return [
            UInt32Type.Default, uint.MinValue, uint.MaxValue, new UInt32Array.Builder().Append(uint.MinValue).AppendNull().Append(uint.MaxValue).Build(),
        ];
        yield return [
            UInt64Type.Default, ulong.MinValue, ulong.MaxValue, new UInt64Array.Builder().Append(ulong.MinValue).AppendNull().Append(ulong.MaxValue).Build(),
        ];
        yield return [
            HalfFloatType.Default, Half.MinValue, Half.MaxValue, new HalfFloatArray.Builder().Append(Half.MinValue).AppendNull().Append(Half.MaxValue).Build(),
        ];
        yield return [
            FloatType.Default, float.MinValue, float.MaxValue, new FloatArray.Builder().Append(float.MinValue).AppendNull().Append(float.MaxValue).Build(),
        ];
        yield return [
            DoubleType.Default, double.MinValue, double.MaxValue, new DoubleArray.Builder().Append(double.MinValue).AppendNull().Append(double.MaxValue).Build(),
        ];
        // yield return [
        //     // new Decimal128Type(10, 6), Decimal128.MinValue, Decimal128.MaxValue, new Decimal128Array.Builder(new Decimal128Type(10, 6)).Append(Decimal128.MinValue).AppendNull().Append(Decimal128.MaxValue).Build(),
        // ];
        // yield return [
        //     // new Decimal256Type(16, 8), Decimal256.MinValue, Decimal256.MaxValue, new Decimal256Array.Builder(new Decimal256Type(16, 8)).Append(Decimal256.MinValue).AppendNull().Append(Decimal256.MaxValue).Build(),
        // ];
        yield return [
            StringType.Default, "a", "z", new StringArray.Builder().Append("a").AppendNull().Append("z").Build(),
        ];
        yield return [
            StringViewType.Default, "a", "z", new StringViewArray.Builder().Append("a").AppendNull().Append("z").Build(),
        ];
        var minDate = DateOnly.MinValue.ToDateTime(new TimeOnly(), DateTimeKind.Utc);
        var maxDate = DateOnly.MaxValue.ToDateTime(new TimeOnly(), DateTimeKind.Utc);
        yield return [
            Date32Type.Default, minDate, maxDate, new Date32Array.Builder().Append(minDate).AppendNull().Append(maxDate).Build(),
        ];
        yield return [
            Date64Type.Default, minDate, maxDate, new Date64Array.Builder().Append(minDate).AppendNull().Append(maxDate).Build(),
        ];
        // yield return [
        //     Time32Type.Default, TimeOnly.MinValue, TimeOnly.MaxValue, new Time32Array.Builder().Append(TimeOnly.MinValue).AppendNull().Append(TimeOnly.MaxValue).Build(),
        // ];
        // yield return [
        //     Time64Type.Default, long.MinValue, long.MaxValue, new Time64Array.Builder().Append(long.MinValue).AppendNull().Append(long.MaxValue).Build(),
        // ];
        // yield return [
        //     DurationType.Second, TimeSpan.MinValue, TimeSpan.MaxValue, new DurationArray.Builder(DurationType.Second).Append(TimeSpan.MinValue).AppendNull().Append(TimeSpan.MaxValue).Build(),
        // ];
        // yield return [
        //     DurationType.Millisecond, TimeSpan.MinValue, TimeSpan.MaxValue, new DurationArray.Builder(DurationType.Millisecond).Append(TimeSpan.MinValue).AppendNull().Append(TimeSpan.MaxValue).Build(),
        // ];
        // yield return [
        //     DurationType.Microsecond, TimeSpan.MinValue, TimeSpan.MaxValue, new DurationArray.Builder(DurationType.Microsecond).Append(TimeSpan.MinValue).AppendNull().Append(TimeSpan.MaxValue).Build(),
        // ];
        // yield return [
        //     DurationType.Nanosecond, TimeSpan.MinValue, TimeSpan.MaxValue, new DurationArray.Builder(DurationType.Nanosecond).Append(TimeSpan.MinValue).AppendNull().Append(TimeSpan.MaxValue).Build(),
        // ];
    }

    [Theory]
    [MemberData(nameof(TestCases))]
    public void Update_WithValidTestCase_ShouldFindMinMaxNull(IArrowType type, object min, object max, IArrowArray array)
    {
        var expected = new DeltaStats(1, [new("x", min)], [new("x", max)], [new("x", 1)]);

        var schema = new Schema([
            new Field("x", type, true)
        ], []);

        var batch = new RecordBatch(schema, [
            array
        ], 1);

        var actual = new DeltaStats();
        actual.Update(batch);

        Assert.Equal(expected, actual);

    }

    [Fact]
    public void Equals_ShouldReturnTrue()
    {
        DeltaStats deltaStats1 = new(1, [new("x", 0)], [new("x", 1)], [new("x", 0)]);
        DeltaStats deltaStats2 = new(1, [new("x", 0)], [new("x", 1)], [new("x", 0)]);

        var equals = deltaStats1.Equals(deltaStats2);

        Assert.True(equals);
    }

    // [Fact]
    // public void GetFileStats_ShouldReturnStats()
    // {
    //     var minValues = new DeltaMap<string, object>
    //     {
    //         ["a0"] = -1,
    //         ["a"] = -1,
    //         ["b0"] = -1L,
    //         ["b"] = -1L,
    //         ["c0"] = -1f,
    //         ["c"] = -1f,
    //         ["d0"] = -1d,
    //         ["d"] = -1d,
    //         ["e0"] = false,
    //         ["e1"] = false,
    //         ["s"] = "alpha",
    //         // ["t1"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    //         ["t2"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    //         ["t3"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),

    //         ["t5"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local),
    //         ["t6"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local),
    //     };
    //     var maxValues = new DeltaMap<string, object>
    //     {
    //         ["a0"] = 1,
    //         ["a"] = 1,
    //         ["b0"] = 1L,
    //         ["b"] = 1L,
    //         ["c0"] = 1f,
    //         ["c"] = 1f,
    //         ["d0"] = 1d,
    //         ["d"] = 1d,
    //         ["e0"] = true,
    //         ["e1"] = true,

    //         ["s"] = "gamma",
    //         // ["t1"] = new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),
    //         ["t2"] = new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),
    //         ["t3"] = new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),

    //         ["t5"] = new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Local),
    //         ["t6"] = new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Local),
    //     };
    //     var nullCount = new DeltaMap<string, long>
    //     {
    //         ["a0"] = 0,
    //         ["a"] = 0,
    //         ["b0"] = 0,
    //         ["b"] = 0,
    //         ["c0"] = 0,
    //         ["c"] = 0,
    //         ["d0"] = 0,
    //         ["d"] = 0,
    //         ["e0"] = 0,
    //         ["e1"] = 0,

    //         ["s"] = 0,
    //         // ["t1"] = 0,
    //         ["t2"] = 0,
    //         ["t3"] = 0,
    //         ["t5"] = 0,
    //         ["t6"] = 0,

    //     };
    //     var expected = new DeltaStats(3, minValues, maxValues, nullCount);

    //     var path = Path.GetTempFileName();

    //     var columns = new Column[]
    //     {
    //         new Column<int>("a0", LogicalType.None()),
    //         new Column<int>("a"),
    //         new Column<long>("b0", LogicalType.None()),
    //         new Column<long>("b"),
    //         new Column<float>("c0", LogicalType.None()),
    //         new Column<float>("c"),
    //         new Column<double>("d0", LogicalType.None()),
    //         new Column<double>("d"),
    //         new Column<bool>("e0"),
    //         new Column<bool>("e1"),
    //         new Column<string>("s"),
    //         // new Column<DateTime>("t1", LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos)),
    //         new Column<DateTime>("t2", LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros)),
    //         new Column<DateTime>("t3", LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Millis)),
    //         new Column<DateTime>("t5", LogicalType.Timestamp(isAdjustedToUtc: false, timeUnit: TimeUnit.Micros)),
    //         new Column<DateTime>("t6", LogicalType.Timestamp(isAdjustedToUtc: false, timeUnit: TimeUnit.Millis)),
    //     };

    //     using (var file = new ParquetFileWriter(path, columns))
    //     using (var rowGroup = file.AppendRowGroup())
    //     {
    //         using (var w = rowGroup.NextColumn().LogicalWriter<int>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<int>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<long>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<long>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<float>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<float>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<double>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<double>())
    //         {
    //             w.WriteBatch([-1, 0, 1]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<bool>())
    //         {
    //             w.WriteBatch([false, false, true]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<bool>())
    //         {
    //             w.WriteBatch([false, false, true]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<string>())
    //         {
    //             w.WriteBatch(["alpha", "beta", "gamma"]);
    //         }
    //         // using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
    //         // {
    //         //     w.WriteBatch([
    //         //         new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    //         //         new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc),
    //         //         new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),
    //         //     ]);
    //         // }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
    //         {
    //             w.WriteBatch([
    //                 new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    //                 new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc),
    //                 new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),
    //             ]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
    //         {
    //             w.WriteBatch([
    //                 new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    //                 new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc),
    //                 new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc),
    //             ]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
    //         {
    //             w.WriteBatch([
    //                 new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local),
    //                 new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Local),
    //                 new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Local),
    //             ]);
    //         }
    //         using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
    //         {
    //             w.WriteBatch([
    //                 new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local),
    //                 new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Local),
    //                 new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Local),
    //             ]);
    //         }
    //         file.Close();
    //     }
    //     var actual = DeltaStats.GetFileStats(path);

    //     Assert.Equal(expected, actual);
    // }
}