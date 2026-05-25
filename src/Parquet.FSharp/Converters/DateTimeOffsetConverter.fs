namespace Parquet.FSharp

open System
open System.Linq.Expressions

// TODO: Handle UTC vs Local for both serialization and deserialization.
type internal DateTimeOffsetConverter private () =
    let dotnetType = typeof<DateTimeOffset>
    let dataDotnetType = typeof<DateTime>
    // TODO: Look for other places where reflection could be extracted from
    // field functions (these are no longer functions, but they used to be!)
    let utcDateTimeProperty = typeof<DateTimeOffset>.GetProperty("UtcDateTime")
    let dateTimeConstructor = typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |])

    let serializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let getDataValue (dateTimeOffset: Expression) =
            // dateTimeOffset.UtcDateTime
            Expression.Property(dateTimeOffset, utcDateTimeProperty)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let createFromDataValue (dateTime: Expression) =
            Expression.New(dateTimeConstructor, dateTime)
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = DateTimeOffsetConverter()

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
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc
                        && dateTimeSchema.Unit = TimeUnit.Microseconds ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None
