namespace Parquet.FSharp

open System
open System.Linq.Expressions

// TODO: Support other TimestampTypes from Parquet.Net
// TODO: Handle UTC vs Local for both serialization and deserialization.
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
    Optional: bool
    Unit: TimeUnit }
    with
    static member val Default = {
        DateTimeConverterSettings.Optional = false
        DateTimeConverterSettings.Unit = TimeUnit.Microseconds }

type internal DateTimeConverter(converterSettings: DateTimeConverterSettings) =
    let dotnetType = typeof<DateTime>
    let dataDotnetType = typeof<DateTime>

    let requiredSerializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = converterSettings.Unit
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let getDataValue (dateTime: Expression) =
            // if dateTime.Kind <> DateTimeKind.Utc then
            //     raise SerializationException(...)
            // dateTime
            let kind = Expression.Property(dateTime, "Kind")
            Expression.Block(
                Expression.IfThen(
                    Expression.NotEqual(kind, Expression.Constant(DateTimeKind.Utc)),
                    Expression.FailWith<SerializationException>(
                        Expression.Constant(
                            "encountered 'DateTime' with 'DateTimeKind."),
                        Expression.Call(kind, "ToString", []),
                        Expression.Constant(
                            "' during serialization of timestamp with instant"
                            + " semantics which only allows 'DateTimeKind.Utc'"
                            + " by default"))),
                dateTime)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = converterSettings.Unit
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = DateTimeConverter(DateTimeConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if sourceType <> dotnetType
            then Option.None
            else
                if converterSettings.Optional
                then option.Some optionalSerializer
                else Option.Some requiredSerializer

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc
                        && dateTimeSchema.Unit = converterSettings.Unit ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
