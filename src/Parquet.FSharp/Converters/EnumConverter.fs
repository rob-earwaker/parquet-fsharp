namespace Parquet.FSharp

open System.Linq.Expressions

// TODO: Support serialization as string value.

type internal EnumConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        EnumConverterSettings.Optional = false }

type internal EnumConverter(converterSettings: EnumConverterSettings) =
    let createRequiredSerializer (enumInfo: EnumInfo) =
        let dotnetType = enumInfo.Type
        // All enum value types are simple primitive atomic values (int8, int16,
        // int32, int64, uint8, uint16, uint32, uint64).
        let dataDotnetType = enumInfo.ValueType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (enum: Expression) =
            Expression.Convert(enum, enumInfo.ValueType)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createOptionalSerializer enumInfo =
        createRequiredSerializer enumInfo
        |> Serializer.optionalNonNullableTypeWrapper

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

    static member val Default = EnumConverter(EnumConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Enum enumInfo ->
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer enumInfo)
                else Option.Some (createRequiredSerializer enumInfo)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Enum enumInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = enumInfo.ValueType ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer enumInfo)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer enumInfo)
                    else Option.None
                | _ -> Option.None
            | _ -> Option.None
