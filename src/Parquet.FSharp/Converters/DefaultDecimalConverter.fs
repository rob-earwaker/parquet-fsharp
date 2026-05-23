namespace Parquet.FSharp

open System.Linq.Expressions

type internal DefaultDecimalConverter private () =
    let dotnetType = typeof<decimal>

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

    static member val Default = DefaultDecimalConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
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
                        || primitiveSchema.DataDotnetType = typeof<int64>
                        || primitiveSchema.DataDotnetType = typeof<int32>
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint64>
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None
