namespace Parquet.FSharp

open System.Linq.Expressions

type internal StringConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        StringConverterSettings.Optional = false
        StringConverterSettings.AllowNull = false }

type internal StringConverter(converterSettings: StringConverterSettings) =
    let dotnetType = typeof<string>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (value: Expression) =
            Expression.Block(
                Serializer.throwIfNull converterSettings.AllowNull value,
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

    static member val Default = StringConverter(StringConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, valueSettings, settings) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                // Only support atomic values with the correct type.
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    // Choose the right deserializer based on whether the values
                    // are optional.
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None
