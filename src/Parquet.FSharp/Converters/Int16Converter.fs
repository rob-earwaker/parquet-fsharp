namespace Parquet.FSharp

type internal Int16ConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        Int16ConverterSettings.Optional = false }

type internal Int16Converter(converterSettings: Int16ConverterSettings) =
    let dotnetType = typeof<int16>
    let dataDotnetType = typeof<int16>

    let requiredSerializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = Int16Converter(Int16ConverterSettings.Default)

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
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dataDotnetType ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
