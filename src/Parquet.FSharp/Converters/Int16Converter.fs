namespace Parquet.FSharp

open System.Linq.Expressions

type internal Int16Converter private () =
    let dotnetType = typeof<int16>

    // TODO: Could support serializing as smaller integer type using an attribute.
    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = Int16Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None
