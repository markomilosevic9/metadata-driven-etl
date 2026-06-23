from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    TimestampType,
    DateType,
)


# schema enforcement module
# translates the json schema definition in metadata into a spark structtype used for explicit schema enforcement at read time


TYPE_MAPPING = {
    "string":    StringType(),
    "integer":   IntegerType(),
    "long":      LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "boolean":   BooleanType(),
    "timestamp": TimestampType(),
    "date":      DateType(),
}


def build_spark_schema(schema_def: dict) -> StructType:
    # convert a json schema definition from metadata into a spark structtype
    # raises ValueError for missing required structure or unsupported type strings
    if schema_def.get("type") != "struct":
        raise ValueError(
            f"Schema type must be 'struct', got '{schema_def.get('type')}'"
        )

    if "fields" not in schema_def:
        raise ValueError("Schema definition missing 'fields' key")

    fields_list = schema_def["fields"]

    if len(fields_list) == 0:
        raise ValueError("Schema must define at least one field")

    spark_fields = []

    for field in fields_list:
        field_name = field["name"]
        field_type = field["type"]
        field_nullable = field["nullable"]

        if field_type not in TYPE_MAPPING:
            raise ValueError(
                f"Unsupported type '{field_type}' for field '{field_name}'. "
                f"Supported types: {sorted(TYPE_MAPPING.keys())}"
            )

        spark_fields.append(StructField(field_name, TYPE_MAPPING[field_type], field_nullable))

    return StructType(spark_fields)
