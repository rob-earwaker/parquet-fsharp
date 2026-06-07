namespace Parquet.FSharp

open System.Linq.Expressions

type internal Float64Converter private () =
    let dotnetType = typeof<float>

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

    static member val Default = Float64Converter()

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
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<float32>
                        || primitiveSchema.DataDotnetType = typeof<int32>
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None
