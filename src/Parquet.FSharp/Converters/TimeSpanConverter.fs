namespace Parquet.FSharp

open System
open System.Linq.Expressions

type internal TimeSpanConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        TimeSpanConverterSettings.Optional = false }

type internal TimeSpanConverter(converterSettings: TimeSpanConverterSettings) =
    let dotnetType = typeof<TimeSpan>
    let dataDotnetType = typeof<int64>

    let ticksProperty = typeof<TimeSpan>.GetProperty("Ticks")
    let ticksConstructor = typeof<TimeSpan>.GetConstructor([| typeof<int64> |])
    let ticksPerMicrosecond = Expression.Constant(TimeSpan.TicksPerMicrosecond)

    let requiredSerializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (timeSpan: Expression) =
            // timeSpan.Ticks / ticksPerMicrosecond
            Expression.Divide(
                Expression.Property(timeSpan, ticksProperty),
                ticksPerMicrosecond)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

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

    static member val Default = TimeSpanConverter(TimeSpanConverterSettings.Default)

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
