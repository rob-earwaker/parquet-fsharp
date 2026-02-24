namespace Parquet.FSharp

open System
open System.Linq.Expressions

// TODO: Handle UTC vs Local for both serialization and deserialization.
type internal DefaultDateTimeOffsetConverter private () =
    let dotnetType = typeof<DateTimeOffset>
    let utcDateTimeProperty = typeof<DateTimeOffset>.GetProperty("UtcDateTime")
    let dateTimeOptionInfo = OptionInfo.ofTypeCached typeof<DateTime option>
    let dateTimeConstructor = typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |])

    let createSerializer settings =
        let dateTimeSerializer = Serializer.resolve typeof<DateTime> settings
        let unwrapValue (dateTimeOffset: Expression) =
            // dateTimeOffset.UtcDateTime
            Expression.Property(dateTimeOffset, utcDateTimeProperty)
            :> Expression
        Serializer.wrapAs dotnetType dateTimeSerializer unwrapValue

    let createDeserializer sourceSchema settings =
        let dateTimeOptionDeserializer =
            Deserializer.resolve sourceSchema dateTimeOptionInfo.Type settings
        let wrapValue (dateTimeOption: Expression) =
            // if dateTimeOption.IsNone then
            //     raise SerializationException(...)
            // DateTimeOffset(dateTimeOption.Value)
            Expression.Block(
                Expression.IfThen(
                    dateTimeOptionInfo.IsNull dateTimeOption,
                    Deserializer.throwNullValueEncounteredForNonNullableType dotnetType),
                Expression.New(
                    dateTimeConstructor,
                    dateTimeOptionInfo.GetValue dateTimeOption))
            :> Expression
        Deserializer.wrapAs dotnetType dateTimeOptionDeserializer wrapValue

    static member Instance = DefaultDateTimeOffsetConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some (createSerializer settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if targetType = dotnetType
            then Option.Some (createDeserializer sourceSchema settings)
            else Option.None
