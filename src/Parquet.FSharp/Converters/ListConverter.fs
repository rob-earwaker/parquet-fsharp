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

    let createRequiredSerializer (listValue: ValueDefinition) settings =
        let elementDotnetType = listValue.Type.GetGenericArguments()[0]
        let elementValue = listValue |> ValueDefinition.forNestedValue elementDotnetType
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
                Serializer.throwIfNull converterSettings.AllowNull list,
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list listValue.Type elementSerializer getEnumerator

    let createOptionalSerializer listValue settings =
        createRequiredSerializer listValue settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (listSchema: ListTypeSchema) (listValue: ValueDefinition) settings =
        let elementDotnetType = listValue.Type.GetGenericArguments()[0]
        let elementValue = listValue |> ValueDefinition.forNestedValue elementDotnetType
        let elementDeserializer =
            Deserializer.resolve listSchema.Element elementValue settings
        let createEmpty =
            Expression.Property(null, listValue.Type.GetProperty("Empty"))
        let createFromElementValues (elementValues: Expression) =
            let seqModuleType =
                System.Reflection.Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "SeqModule")
                |> Array.exactlyOne
            Expression.Call(seqModuleType, "ToList", [| elementDotnetType |], elementValues)
            :> Expression
        Deserializer.list
            listValue.Type elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer listSchema listValue settings =
        createRequiredDeserializer listSchema listValue settings
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
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer listSchema targetValue settings)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer listSchema targetValue settings)
                    else Option.None
                | _ -> Option.None
