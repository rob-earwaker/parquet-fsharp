namespace Parquet.FSharp

open System
open System.Collections.Generic
open System.Linq.Expressions

type internal ListConverter private () =
    let isListType = DotnetType.isGenericType<list<_>>

    let createSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType settings
        let getEnumerator (list: Expression) =
            // let enumerable = list :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list dotnetType elementSerializer getEnumerator

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
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = ListConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if isListType sourceType
            then Option.Some (createSerializer sourceType settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isListType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    else Option.Some (createRequiredDeserializer listSchema targetType settings)
                | _ -> Option.None
