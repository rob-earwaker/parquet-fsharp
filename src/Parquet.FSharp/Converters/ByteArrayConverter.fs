namespace Parquet.FSharp

open System.Linq.Expressions

type internal ByteArrayConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        ByteArrayConverterSettings.Optional = false
        ByteArrayConverterSettings.AllowNull = false }

type internal ByteArrayConverter(converterSettings: ByteArrayConverterSettings) =
    let dotnetType = typeof<byte[]>
    let dataDotnetType = typeof<byte[]>

    let requiredSerializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (value: Expression) =
            Expression.Block(
                Serializer.throwIfNull converterSettings.Optional value,
                value)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ByteArrayConverter(ByteArrayConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if sourceValue.Type <> dotnetType
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some optionalSerializer
                else Option.Some requiredSerializer

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if targetValue.Type <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                // Only support atomic values with the correct type.
                | ValueTypeSchema.Primitive schema
                    // TODO: Support reading binary-backed types, e.g. Guid, string?
                    when schema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
