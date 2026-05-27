namespace Parquet.FSharp

open System.Linq.Expressions

// TODO: Add back support for conversions to other backing types, e.g.
// enum<int16> serialized as int32.

type internal EnumConverter private () =
    let createSerializer (enumInfo: EnumInfo) =
        let dotnetType = enumInfo.Type
        // All enum value types are simple primitive atomic values (int8, int16,
        // int32, int64, uint8, uint16, uint32, uint64).
        let dataDotnetType = enumInfo.ValueType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (enum: Expression) =
            Expression.Convert(enum, enumInfo.ValueType)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer (enumInfo: EnumInfo) =
        let dotnetType = enumInfo.Type
        // All enum value types are simple primitive atomic values (int8, int16,
        // int32, int64, uint8, uint16, uint32, uint64).
        let dataDotnetType = enumInfo.ValueType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            Expression.Convert(dataValue, enumInfo.Type)
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer enumInfo =
        createRequiredDeserializer enumInfo
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = EnumConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            match sourceType with
            | DotnetType.Enum enumInfo ->
                Option.Some (createSerializer enumInfo)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, valueSettings, settings) =
            match targetType with
            | DotnetType.Enum enumInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = enumInfo.ValueType ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer enumInfo)
                    else Option.Some (createRequiredDeserializer enumInfo)
                | _ -> Option.None
            | _ -> Option.None
