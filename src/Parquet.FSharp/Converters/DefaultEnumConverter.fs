namespace Parquet.FSharp

open System.Linq.Expressions

type internal DefaultEnumConverter private () =
    let createSerializer sourceType settings =
        let intSerializer = Serializer.resolve typeof<int> settings
        let unwrapValue (enum: Expression) =
            Expression.Convert(enum, typeof<int>)
            :> Expression
        Serializer.wrapAs sourceType intSerializer unwrapValue

    let createDeserializer sourceSchema targetType settings =
        let intOptionDeserializer =
            Deserializer.resolve sourceSchema typeof<int option> settings
        let optionInfo = OptionInfo.ofTypeCached typeof<int option>
        let wrapValue (intOption: Expression) =
            Expression.Block(
                Expression.IfThen(
                    optionInfo.IsNull intOption,
                    Deserializer.throwNullValueEncounteredForNonNullableType targetType),
                Expression.Convert(optionInfo.GetValue intOption, targetType))
            :> Expression
        Deserializer.wrapAs targetType intOptionDeserializer wrapValue

    static member Instance = DefaultEnumConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Enum ->
                Option.Some (createSerializer sourceType settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Enum ->
                Option.Some (createDeserializer sourceSchema targetType settings)
            | _ -> Option.None
