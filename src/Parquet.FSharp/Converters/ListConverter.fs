namespace Parquet.FSharp

open System
open System.Collections.Generic
open System.Linq.Expressions

// TODO: Should we allow invalid or non-sensical combinations, e.g. allow null
// without optional?

type internal ListConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        ListConverterSettings.Optional = false
        ListConverterSettings.AllowNull = false }

type internal ListConverter(converterSettings: ListConverterSettings) =
    let isListType = DotnetType.isGenericType<list<_>>

    let createRequiredSerializer (dotnetType: Type) (listSettings: ValueSettings) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSettings = listSettings.NestedValueSettings
        let elementSerializer =
            Serializer.resolveWithValueSettings elementDotnetType elementSettings settings
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
                Serializer.throwIfNull converterSettings.AllowNull list,
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list dotnetType elementSerializer getEnumerator

    let createOptionalSerializer dotnetType listSettings settings =
        createRequiredSerializer dotnetType listSettings settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer
        (schema: ListTypeSchema) (dotnetType: Type) (listSettings: ValueSettings) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSettings = listSettings.NestedValueSettings
        let elementDeserializer =
            Deserializer.resolveWithValueSettings
                schema.Element elementDotnetType elementSettings settings
        let createEmpty =
            Expression.Property(null, dotnetType.GetProperty("Empty"))
        let createFromElementValues (elementValues: Expression) =
            let seqModuleType =
                System.Reflection.Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "SeqModule")
                |> Array.exactlyOne
            Expression.Call(seqModuleType, "ToList", [| elementDotnetType |], elementValues)
            :> Expression
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType listSettings settings =
        createRequiredDeserializer schema dotnetType listSettings settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ListConverter(ListConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if not (isListType sourceType)
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer sourceType valueSettings settings)
                else Option.Some (createRequiredSerializer sourceType valueSettings settings)

        member this.TryCreateDeserializer(sourceSchema, targetType, valueSettings, settings) =
            if not (isListType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer listSchema targetType valueSettings settings)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer listSchema targetType valueSettings settings)
                    else Option.None
                | _ -> Option.None
