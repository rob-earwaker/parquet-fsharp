namespace Parquet.FSharp

type internal UInt32ConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        UInt32ConverterSettings.Optional = false }

type internal UInt32Converter(converterSettings: UInt32ConverterSettings) =
    let dotnetType = typeof<uint32>
    let dataDotnetType = typeof<uint32>

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

    static member val Default = UInt32Converter(UInt32ConverterSettings.Default)

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
