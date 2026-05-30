namespace Parquet.FSharp

open System
open System.Linq.Expressions

type internal TimeSpanConverter private () =
    let dotnetType = typeof<TimeSpan>
    let dataDotnetType = typeof<int64>
    let ticksProperty = typeof<TimeSpan>.GetProperty("Ticks")
    let ticksConstructor = typeof<TimeSpan>.GetConstructor([| typeof<int64> |])
    let ticksPerMicrosecond = Expression.Constant(10L)

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (timeSpan: Expression) =
            // timeSpan.Ticks / ticksPerMicrosecond
            Expression.Divide(
                Expression.Property(timeSpan, ticksProperty),
                ticksPerMicrosecond)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (microseconds: Expression) =
            // TimeSpan(microseconds * ticksPerMicrosecond)
            Expression.New(
                ticksConstructor,
                Expression.Multiply(microseconds, ticksPerMicrosecond))
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = TimeSpanConverter()

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
                    when primitiveSchema.DataDotnetType = dataDotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None
