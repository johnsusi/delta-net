<p align="center">
    <img src="https://raw.githubusercontent.com/johnsusi/delta-net/main/docs/logo.svg" alt="delta-net logo" height="200">
</p>
<p align="center">
A dotnet library for Delta Lake.
</p>

## Introduction

This project uses Delta Lake, an open-source storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to big data workloads.

Delta Lake provides the ability to perform batch and streaming workloads on a single platform with high reliability and performance. It offers schema enforcement and evolution, ensuring data integrity. It also provides a full historical audit trail of all the changes made to the data.

## Getting Started

### Install the package

```pwsh
dotnet add package DeltaLake
```

## Usage

### Reading a table

```csharp
using DeltaLake;

var table = new DeltaTable.Builder()
    .WithFileSystem("file:///path/to/table")
    .Build();

await foreach (var batch in table.GetRecordBatches())
{
    Console.WriteLine(batch);
}

```

### Reading a typed table

```csharp
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake;

record FooTable(int Id, string? Value) : ITable<FooTable>
{
    public static Schema Schema { get; } = new([
        new("id", Int32Type.Default, false, []),
        new("value", StringType.Default, true, [])
    ], []);

    public static IEnumerable<FooTable> Enumerate(RecordBatch batch)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var idArray = batch.Column(0) as IReadOnlyList<int?> ?? throw new Exception("Expected non-null array");
            var valueArray = batch.Column(1) as IReadOnlyList<string?> ?? throw new Exception("Expected non-null array");
            yield return new FooTable(idArray[i] ?? throw new Exception("Cannot be null"), valueArray[i]);
        }
    }

var table = new DeltaTable<FooTable>.Builder()
    .WithFileSystem("file:///path/to/table")
    .Build();

await foreach (var row in table.ReadAll())
{
    Console.WriteLine($"Id: {row.Id}, Value: {row.Value}");
}



```

### Create a table

```csharp
using DeltaLake;

var table = new DeltaTable.Builder()
    .WithFileSystem("file:///path/to/table")
    .WithSchema(schema)
    .EnsureCreated()
    .Build();

```

### Update a table

```csharp
using DeltaLake;

var table = ...;

using var data = new RecordBatch(table.Schema, [
    new Int32Array
        .Builder()
        .Append(1)
        .Append(2)
        .Append(3)
        .Build(),
    new StringArray
        .Builder()
        .Append("one")
        .AppendNull()
        .Append("two")
        .Build(),
], 3);

table = new DeltaTable.Builder()
    .FromTable(table)
    .Add(data)
    .Build();

```


