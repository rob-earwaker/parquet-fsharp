namespace Parquet.FSharp

open System
open System.Linq.Expressions

// TODO: Handle UTC vs Local for both serialization and deserialization.
type internal DefaultDateTimeOffsetConverter private () =
    let dotnetType = typeof<DateTimeOffset>
    let dataDotnetType = typeof<DateTime>

    // TODO: Maybe should resolve a DateTime serializer/deserializer and wrap?

    let serializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            let unit = TimeUnit.Microseconds
            ValueTypeSchema.dateTime isAdjustedToUtc unit
        let createFromDataValue (dateTime: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                dateTime)
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member Instance = DefaultDateTimeOffsetConverter()

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
