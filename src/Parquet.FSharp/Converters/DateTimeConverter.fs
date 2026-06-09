namespace Parquet.FSharp

open System
open System.Linq.Expressions

// TODO: Support other TimestampTypes from Parquet.Net?
// ---
// Parquet.Net behaviour:
//
// DateTime (no attribute)
//   => INT96
//   => serialization ignores Kind, no truncation
//   => deserialization assumes UTC
//
// DateTime [ParquetTimestamp(<resolution>, logical=false, <utc-adjusted-ignored>>)]
//   => INT64, TIMESTAMP_<resolution>, (no logical type)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes UTC

// DateTime [ParquetTimestamp(<resolution>, logical=true, utcAdjusted=true)]
//   => INT64, (no converted type), TIMESTAMP(unit: <resolution>, isAdjustedToUtc: true)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes UTC

// DateTime [ParquetTimestamp(<resolution>, logical=true, utcAdjusted=false)]
//   => INT64, (no converted type), TIMESTAMP(unit: <resolution>, isAdjustedToUtc: false)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes Local

type internal DateTimeConverterSettings = {
    Unit: TimeUnit
    Local: bool
    Optional: bool }
    with
    static member val Default = {
        DateTimeConverterSettings.Unit = TimeUnit.Microseconds
        DateTimeConverterSettings.Local = false
        DateTimeConverterSettings.Optional = false }

// TODO: Allow date time kind to be ignored

type internal DateTimeConverter(converterSettings: DateTimeConverterSettings) =
    let dotnetType = typeof<DateTime>
    let dataDotnetType = typeof<DateTime>
    let isAdjustedToUtc = not converterSettings.Local

    let requiredSerializer =
        let schema = ValueTypeSchema.dateTime isAdjustedToUtc converterSettings.Unit
        let getDataValue (dateTime: Expression) =
            let expectedKind, semanticName =
                if isAdjustedToUtc
                then DateTimeKind.Utc, "instant"
                else DateTimeKind.Local, "local"
            // if dateTime.Kind <> expectedKind then
            //     raise SerializationException(...)
            // dateTime
            let kind = Expression.Property(dateTime, "Kind")
            Expression.Block(
                Expression.IfThen(
                    Expression.NotEqual(kind, Expression.Constant(expectedKind)),
                    Expression.FailWith<SerializationException>(
                        Expression.Constant(
                            "encountered 'DateTime' with 'DateTimeKind."),
                        Expression.Call(kind, "ToString", []),
                        Expression.Constant(
                            $"' during serialization of timestamp with {semanticName}"
                            + $" semantics which only allows 'DateTimeKind.{expectedKind}'"
                            + " by default"))),
                dateTime)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema = ValueTypeSchema.dateTime isAdjustedToUtc converterSettings.Unit
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = DateTimeConverter(DateTimeConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if sourceValue.Type <> dotnetType
            then Option.None
            else
                if converterSettings.Optional
                then option.Some optionalSerializer
                else Option.Some requiredSerializer

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if targetValue.Type <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc = isAdjustedToUtc
                        && dateTimeSchema.Unit = converterSettings.Unit ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
