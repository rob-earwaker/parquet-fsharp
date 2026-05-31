namespace Parquet.FSharp

open System
open System.Collections.Generic
open System.Linq.Expressions

type internal ListConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        ListConverterSettings.Optional = false
        ListConverterSettings.AllowNull = false }

type internal ListConverter(converterSettings: ListConverterSettings) =
    let isListType = DotnetType.isGenericType<list<_>>

    let createRequiredSerializer (value: ValueDefinition) settings =
        let elementDotnetType = value.Type.GetGenericArguments()[0]
        let elementValue = value |> ValueDefinition.forNestedValue elementDotnetType
        let elementSerializer = Serializer.resolve elementValue settings
        let getEnumerator (list: Expression) =
            // if isNull list then
            //     raise SerializationException(...)
            // let enumerable = list :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Serializer.throwIfNull converterSettings.Optional list,
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list value.Type elementSerializer getEnumerator

    let createOptionalSerializer value settings =
        createRequiredSerializer value settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let createRequiredDeserializer (schema: ListTypeSchema) (value: ValueDefinition) settings =
        let elementDotnetType = value.Type.GetGenericArguments()[0]
        let elementValue = value |> ValueDefinition.forNestedValue elementDotnetType
        let elementDeserializer =
            Deserializer.resolve schema.Element elementValue settings
        let createEmpty =
            Expression.Property(null, value.Type.GetProperty("Empty"))
        let createFromElementValues (elementValues: Expression) =
            let seqModuleType =
                System.Reflection.Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "SeqModule")
                |> Array.exactlyOne
            Expression.Call(seqModuleType, "ToList", [| elementDotnetType |], elementValues)
            :> Expression
        Deserializer.list
            value.Type elementDeserializer createEmpty createFromElementValues

    let createOptionalDeserializer schema value settings =
        createRequiredDeserializer schema value settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ListConverter(ListConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if not (isListType sourceValue.Type)
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer sourceValue settings)
                else Option.Some (createRequiredSerializer sourceValue settings)

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if not (isListType targetValue.Type)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List schema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer schema targetValue settings)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer schema targetValue settings)
                    else Option.None
                | _ -> Option.None
