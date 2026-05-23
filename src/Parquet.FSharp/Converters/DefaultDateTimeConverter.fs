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

type internal DefaultDateTimeConverter private () =
    let dotnetType = typeof<DateTime>
    let dataDotnetType = dotnetType

    let serializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
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

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Instance = DefaultDateTimeConverter()

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
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc
                        && dateTimeSchema.Unit = TimeUnit.Microseconds ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None
