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
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (value: Expression) =
            Expression.Block(
                Serializer.throwIfNull converterSettings.Optional value,
                value)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ByteArrayConverter(ByteArrayConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if sourceValue.Type = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if targetValue.Type <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                // Only support atomic values with the correct type.
                | ValueTypeSchema.Primitive primitiveSchema
                    // TODO: Support reading binary-backed types, e.g. Guid, string?
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
