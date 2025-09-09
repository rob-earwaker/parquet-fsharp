namespace Parquet.FSharp.Thrift

open Parquet.FSharp
open System

module SchemaElement =
    let root numChildren =
        SchemaElement(
            Name = "root",
            Num_children = numChildren)
        
    let private group repetitionType name numChildren convertedType logicalType =
        let schemaElement =
            SchemaElement(
                Repetition_type = repetitionType,
                Name = name,
                Num_children = numChildren)
        if Option.isSome convertedType then
            schemaElement.Converted_type <- convertedType.Value
        if Option.isSome logicalType then
            schemaElement.LogicalType <- logicalType.Value
        schemaElement

    let recordGroup repetitionType name numChildren =
        let convertedType = Option.None
        let logicalType = Option.None
        group repetitionType name numChildren convertedType logicalType

    let listOuter repetitionType name =
        let numChildren = 1
        let convertedType = Option.Some ConvertedType.LIST
        let logicalType = Option.Some (LogicalType(LIST = ListType()))
        group repetitionType name numChildren convertedType logicalType

    let listMiddle () =
        let repetitionType = FieldRepetitionType.REPEATED
        let name = "list"
        let numChildren = 1
        let convertedType = Option.None
        let logicalType = Option.None
        group repetitionType name numChildren convertedType logicalType

    let primitive repetitionType name type' =
        SchemaElement(
            Type = type',
            Repetition_type = repetitionType,
            Name = name)

    let bool repetitionType name =
        primitive repetitionType name Type.BOOLEAN

    let float repetitionType name =
        primitive repetitionType name Type.FLOAT

    let double repetitionType name =
        primitive repetitionType name Type.DOUBLE

    let logical repetitionType name logicalType =
        let schemaElement =
            SchemaElement(
                Repetition_type = repetitionType,
                Name = name,
                LogicalType = logicalType)
        // Where applicable, fill in the {type'}, {type_length},
        // {converted_type}, {scale} and {precision} fields fields based on the
        // logical type.
        if logicalType.__isset.STRING then
            schemaElement.Type <- Type.BYTE_ARRAY
            schemaElement.Converted_type <- ConvertedType.UTF8
        elif logicalType.__isset.MAP then
            failwith $"unsupported logical type %A{logicalType}"
        elif logicalType.__isset.LIST then
            failwith $"unsupported logical type %A{logicalType}"
        elif logicalType.__isset.ENUM then
            schemaElement.Type <- Type.BYTE_ARRAY
            schemaElement.Converted_type <- ConvertedType.ENUM
        elif logicalType.__isset.DECIMAL then
            let decimalType = logicalType.DECIMAL
            if decimalType.Precision <= 0 then
                failwith $"invalid decimal precision {decimalType.Precision}"
            elif decimalType.Precision <= 9 then
                schemaElement.Type <- Type.INT32
            elif decimalType.Precision <= 18 then
                schemaElement.Type <- Type.INT64
            else
                // Length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits.
                //   => log_10(2^(8*n - 1) - 1) = p
                //   => 2^(8*n - 1) - 1 = 10^p
                //   => 2^(8*n - 1) = 10^p + 1
                //   => 8*n - 1 = log_2(10^p + 1)
                //   => 8*n = log_2(10^p + 1) + 1
                //   => n = (log_2(10^p + 1) + 1)/8
                // Precision p requires >= ceil((log_2(10^p + 1) + 1)/8) bytes to store.
                let n = int (ceil ((Math.Log(10. ** decimalType.Precision + 1., 2) + 1.) / 8.))
                schemaElement.Type <- Type.FIXED_LEN_BYTE_ARRAY
                schemaElement.Type_length <- n
            schemaElement.Converted_type <- ConvertedType.DECIMAL
            schemaElement.Scale <- decimalType.Scale
            schemaElement.Precision <- decimalType.Precision
        elif logicalType.__isset.DATE then
            schemaElement.Type <- Type.INT32
            schemaElement.Converted_type <- ConvertedType.DATE
        elif logicalType.__isset.TIME then
            if logicalType.TIME.Unit.__isset.MILLIS then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.TIME_MILLIS
            elif logicalType.TIME.Unit.__isset.MICROS then
                schemaElement.Type <- Type.INT64
                schemaElement.Converted_type <- ConvertedType.TIME_MICROS
            elif logicalType.TIME.Unit.__isset.NANOS then
                schemaElement.Type <- Type.INT64
        elif logicalType.__isset.TIMESTAMP then
            schemaElement.Type <- Type.INT64
            if logicalType.TIMESTAMP.Unit.__isset.MILLIS then
                schemaElement.Converted_type <- ConvertedType.TIMESTAMP_MILLIS
            elif logicalType.TIMESTAMP.Unit.__isset.MICROS then
                schemaElement.Converted_type <- ConvertedType.TIMESTAMP_MICROS
        elif logicalType.__isset.INTEGER then
            let bitWidth = logicalType.INTEGER.BitWidth
            let isSigned = logicalType.INTEGER.IsSigned
            if bitWidth = 8y && isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.INT_8
            elif bitWidth = 16y && isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.INT_16
            elif bitWidth = 32y && isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.INT_32
            elif bitWidth = 64y && isSigned then
                schemaElement.Type <- Type.INT64
                schemaElement.Converted_type <- ConvertedType.INT_64
            elif bitWidth = 8y && not isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.UINT_8
            elif bitWidth = 16y && not isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.UINT_16
            elif bitWidth = 32y && not isSigned then
                schemaElement.Type <- Type.INT32
                schemaElement.Converted_type <- ConvertedType.UINT_32
            elif bitWidth = 64y && not isSigned then
                schemaElement.Type <- Type.INT64
                schemaElement.Converted_type <- ConvertedType.UINT_64
            else
                failwith $"unsupported int type %A{logicalType.INTEGER}"
        elif logicalType.__isset.UNKNOWN then
            failwith $"unsupported logical type %A{logicalType}"
        elif logicalType.__isset.JSON then
            schemaElement.Type <- Type.BYTE_ARRAY
            schemaElement.Converted_type <- ConvertedType.JSON
        elif logicalType.__isset.BSON then
            schemaElement.Type <- Type.BYTE_ARRAY
            schemaElement.Converted_type <- ConvertedType.BSON
        elif logicalType.__isset.UUID then
            schemaElement.Type <- Type.FIXED_LEN_BYTE_ARRAY
            schemaElement.Type_length <- 16
        elif logicalType.__isset.FLOAT16 then
            schemaElement.Type <- Type.FIXED_LEN_BYTE_ARRAY
            schemaElement.Type_length <- 2
        elif logicalType.__isset.VARIANT then
            failwith $"unsupported logical type %A{logicalType}"
        elif logicalType.__isset.GEOMETRY then
            failwith $"unsupported logical type %A{logicalType}"
        elif logicalType.__isset.GEOGRAPHY then
            failwith $"unsupported logical type %A{logicalType}"
        schemaElement

module FileMetaData =
    let create version schemaElements =
        FileMetaData(
            Version = version,
            Schema = schemaElements)

    let rec private generateSchemaElements (field: FieldType) =
        seq {
            let repetitionType =
                if field.Value.IsRequired
                then FieldRepetitionType.REQUIRED
                else FieldRepetitionType.OPTIONAL
            let name = field.Name
            match field.Value.Type with
            | ValueType.Bool ->
                yield SchemaElement.bool repetitionType name
            | ValueType.Int32 ->
                let intType = IntType(BitWidth = 32y, IsSigned = true)
                let logicalType = LogicalType(INTEGER = intType)
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.Float64 ->
                yield SchemaElement.double repetitionType name
            | ValueType.DateTimeOffset ->
                let timestampType =
                    TimestampType(
                        IsAdjustedToUTC = true,
                        Unit = TimeUnit(MICROS = MicroSeconds()))
                let logicalType = LogicalType(TIMESTAMP = timestampType)
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.String ->
                let logicalType = LogicalType(STRING = StringType())
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.Array arrayType ->
                yield SchemaElement.listOuter repetitionType name
                yield SchemaElement.listMiddle ()
                let elementField = FieldType.create "element" arrayType.Element
                yield! generateSchemaElements elementField
            | ValueType.Record recordType ->
                yield SchemaElement.recordGroup repetitionType name recordType.Fields.Length
                for field in recordType.Fields do
                    yield! generateSchemaElements field
        }

    let ofSchema (schema: Schema) =
        let version = 1
        let numChildren = schema.Fields.Length
        let rootElement = SchemaElement.root numChildren
        let schemaElements = ResizeArray([| rootElement |])
        for field in schema.Fields do
            schemaElements.AddRange(generateSchemaElements field)
        create version schemaElements
