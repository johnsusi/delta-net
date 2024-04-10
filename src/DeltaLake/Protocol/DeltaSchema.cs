using Apache.Arrow;
using Apache.Arrow.Types;

namespace DeltaLake.Protocol;

public sealed record class DeltaSchema
{
    public string Type { get; } = "struct";

    public DeltaArray<DeltaSchemaField> Fields { get; } = [];

    public DeltaSchema(string type, DeltaArray<DeltaSchemaField> fields)
    {
        if (type != "struct")
        {
            throw new ArgumentException("Type must be struct", nameof(type));
        }
        Type = type;
        Fields = fields;
    }

    public static DeltaSchema FromArrow(Schema schema)
    {
        DeltaArray<DeltaSchemaField> fields = [];
        foreach (var field in schema.FieldsList)
        {
            var type = field.DataType.TypeId switch
            {
                ArrowTypeId.Int8 => "byte",
                ArrowTypeId.Int16 => "short",
                ArrowTypeId.Int32 => "integer",
                ArrowTypeId.Int64 => "long",
                ArrowTypeId.UInt8 => "short",
                ArrowTypeId.UInt16 => "integer",
                ArrowTypeId.UInt32 => "long",
                ArrowTypeId.UInt64 => "long", // decimal or binary?
                ArrowTypeId.Float => "float",
                ArrowTypeId.Double => "double",
                ArrowTypeId.String => "string",
                ArrowTypeId.Binary => "binary",
                ArrowTypeId.Boolean => "boolean",
                ArrowTypeId.Timestamp => "timestamp",
                ArrowTypeId.Date32 => "date",
                ArrowTypeId.Date64 => "date",
                ArrowTypeId.List => "array",
                ArrowTypeId.FixedSizeList => "array",
                ArrowTypeId.Struct => "struct",
                ArrowTypeId.Map => "map",
                ArrowTypeId.Dictionary => "map",

                ArrowTypeId.Time32 => throw new NotImplementedException(),
                ArrowTypeId.Time64 => throw new NotImplementedException(),
                ArrowTypeId.Interval => throw new NotImplementedException(),
                ArrowTypeId.Decimal128 => throw new NotImplementedException(),
                ArrowTypeId.Decimal256 => throw new NotImplementedException(),
                ArrowTypeId.Union => throw new NotImplementedException(),
                ArrowTypeId.Duration => throw new NotImplementedException(),
                ArrowTypeId.Null => throw new NotImplementedException(),
                ArrowTypeId.HalfFloat => throw new NotImplementedException(),
                ArrowTypeId.FixedSizedBinary => throw new NotImplementedException(),
                _ => throw new NotImplementedException()

            };

            fields.Add(new DeltaSchemaField(field.Name, type, field.IsNullable, []));
        }
        return new DeltaSchema("struct", fields);
    }

}