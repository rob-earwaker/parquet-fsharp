namespace Parquet.FSharp

open System.Linq.Expressions

type internal DefaultEnumConverter private () =
    let createSerializer (enumInfo: EnumInfo) settings =
        let valueSerializer = Serializer.resolve enumInfo.ValueType settings
        let unwrapValue (enum: Expression) =
            Expression.Convert(enum, enumInfo.ValueType)
            :> Expression
        Serializer.wrapAs enumInfo.Type valueSerializer unwrapValue

    let createDeserializer sourceSchema (enumInfo: EnumInfo) settings =
        let valueOptionDeserializer =
            Deserializer.resolve sourceSchema enumInfo.ValueOptionInfo.Type settings
        let wrapValue (valueOption: Expression) =
            Expression.Block(
                Expression.IfThen(
                    enumInfo.ValueOptionInfo.IsNull valueOption,
                    Deserializer.throwNullValueEncounteredForNonNullableType enumInfo.Type),
                Expression.Convert(enumInfo.ValueOptionInfo.GetValue valueOption, enumInfo.Type))
            :> Expression
        Deserializer.wrapAs enumInfo.Type valueOptionDeserializer wrapValue

    static member Instance = DefaultEnumConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Enum enumInfo ->
                Option.Some (createSerializer enumInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Enum enumInfo ->
                Option.Some (createDeserializer sourceSchema enumInfo settings)
            | _ -> Option.None
