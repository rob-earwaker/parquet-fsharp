namespace Parquet.FSharp

open System.Linq.Expressions

// TODO: Settings for scale and precision.

type internal DecimalConverter private () =
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

    static member val Default = DecimalConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if sourceValue.Type = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if targetValue.Type <> dotnetType
            then Option.None
            else
                let dataDotnetType =
                    match sourceSchema.Type with
                    | ValueTypeSchema.Decimal decimalSchema -> Option.Some typeof<decimal>
                    | ValueTypeSchema.Primitive primitiveSchema
                        when primitiveSchema.DataDotnetType = typeof<int64>
                            || primitiveSchema.DataDotnetType = typeof<int32>
                            || primitiveSchema.DataDotnetType = typeof<int16>
                            || primitiveSchema.DataDotnetType = typeof<int8>
                            || primitiveSchema.DataDotnetType = typeof<uint64>
                            || primitiveSchema.DataDotnetType = typeof<uint32>
                            || primitiveSchema.DataDotnetType = typeof<uint16>
                            || primitiveSchema.DataDotnetType = typeof<uint8> ->
                            Option.Some primitiveSchema.DataDotnetType
                    | _ -> Option.None
                match dataDotnetType with
                | Option.None -> Option.None
                | Option.Some dataDotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer dataDotnetType)
                    else Option.Some (createRequiredDeserializer dataDotnetType)
