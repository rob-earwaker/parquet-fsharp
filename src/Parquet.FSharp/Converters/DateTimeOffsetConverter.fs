namespace Parquet.FSharp

open System
open System.Linq.Expressions

type internal DateTimeOffsetConverterSettings = {
    Unit: TimeUnit
    Optional: bool }
    with
    static member val Default = {
        DateTimeOffsetConverterSettings.Unit = TimeUnit.Microseconds
        DateTimeOffsetConverterSettings.Optional = false }

type internal DateTimeOffsetConverter(converterSettings: DateTimeOffsetConverterSettings) =
    let dotnetType = typeof<DateTimeOffset>
    let dataDotnetType = typeof<DateTime>
    let isAdjustedToUtc = true

    // TODO: Look for other places where reflection could be extracted from
    // field functions (these are no longer functions, but they used to be!)
    let utcDateTimeProperty = typeof<DateTimeOffset>.GetProperty("UtcDateTime")
    let dateTimeConstructor = typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |])

    let requiredSerializer =
        let schema = ValueTypeSchema.dateTime isAdjustedToUtc converterSettings.Unit
        let getDataValue (dateTimeOffset: Expression) =
            Expression.Property(dateTimeOffset, utcDateTimeProperty)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema = ValueTypeSchema.dateTime isAdjustedToUtc converterSettings.Unit
        let createFromDataValue (dateTime: Expression) =
            Expression.New(dateTimeConstructor, dateTime)
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = DateTimeOffsetConverter(DateTimeOffsetConverterSettings.Default)

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
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc = isAdjustedToUtc
                        && dateTimeSchema.Unit = converterSettings.Unit ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
