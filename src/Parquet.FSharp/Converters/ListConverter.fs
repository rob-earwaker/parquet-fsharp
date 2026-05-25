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

    let createRequiredSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType settings
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

    let createOptionalSerializer dotnetType settings =
        createRequiredSerializer dotnetType settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType settings
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
    let createOptionalDeserializer schema dotnetType settings =
        createRequiredDeserializer schema dotnetType settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ListConverter(ListConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if not (isListType sourceType)
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer sourceType settings)
                else Option.Some (createRequiredSerializer sourceType settings)

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isListType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer listSchema targetType settings)
                    else Option.None
                | _ -> Option.None
