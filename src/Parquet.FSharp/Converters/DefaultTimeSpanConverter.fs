namespace Parquet.FSharp

open System
open System.Linq.Expressions

type internal DefaultTimeSpanConverter private () =
    let dotnetType = typeof<TimeSpan>
    let ticksProperty = typeof<TimeSpan>.GetProperty("Ticks")
    let microsecondsOptionInfo = OptionalInfo.ofOptionTypeCached typeof<int64 option>
    let ticksConstructor = typeof<TimeSpan>.GetConstructor([| typeof<int64> |])
    let ticksPerMicrosecond = Expression.Constant(10L)

    let createSerializer settings =
        let microsecondsSerializer = Serializer.resolve typeof<int64> settings
        let unwrapValue (timeSpan: Expression) =
            // timeSpan.Ticks / ticksPerMicrosecond
            Expression.Divide(
                Expression.Property(timeSpan, ticksProperty),
                ticksPerMicrosecond)
            :> Expression
        Serializer.wrapAs dotnetType microsecondsSerializer unwrapValue

    let createDeserializer sourceSchema settings =
        let microsecondsOptionDeserializer =
            Deserializer.resolve sourceSchema microsecondsOptionInfo.Type settings
        let wrapValue (microsecondsOption: Expression) =
            // if microsecondsOption.IsNone then
            //     raise SerializationException(...)
            // TimeSpan(microsecondsOption.Value * ticksPerMicrosecond)
            Expression.Block(
                Expression.IfThen(
                    microsecondsOptionInfo.IsNull microsecondsOption,
                    Deserializer.throwNullValueEncounteredForNonNullableType dotnetType),
                Expression.New(
                    ticksConstructor,
                    Expression.Multiply(
                        microsecondsOptionInfo.GetValue microsecondsOption,
                        ticksPerMicrosecond)))
            :> Expression
        Deserializer.wrapAs dotnetType microsecondsOptionDeserializer wrapValue

    static member val Default = DefaultTimeSpanConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some (createSerializer settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if targetType = dotnetType
            then Option.Some (createDeserializer sourceSchema settings)
            else Option.None
