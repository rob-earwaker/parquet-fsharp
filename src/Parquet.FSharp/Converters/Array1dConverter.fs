namespace Parquet.FSharp

open System
open System.Collections.Generic
open System.Linq.Expressions

type internal Array1dConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        Array1dConverterSettings.Optional = false
        Array1dConverterSettings.AllowNull = false }

type internal Array1dConverter(converterSettings: Array1dConverterSettings) =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createRequiredSerializer arrayValue settings =
        let elementDotnetType = arrayValue.Type.GetElementType()
        let elementValue = arrayValue |> ValueDefinition.forNestedValue elementDotnetType
        let elementSerializer = Serializer.resolve elementValue settings
        let getEnumerator (array: Expression) =
            // if isNull array then
            //     raise SerializationException(...)
            // let enumerable = array :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Serializer.throwIfNull converterSettings.AllowNull array,
                Expression.Assign(enumerable, Expression.Convert(array, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list arrayValue.Type elementSerializer getEnumerator

    let createOptionalSerializer arrayValue settings =
        createRequiredSerializer arrayValue settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let createRequiredDeserializer (listSchema: ListTypeSchema) (arrayValue: ValueDefinition) settings =
        let elementDotnetType = arrayValue.Type.GetElementType()
        let elementValue = arrayValue |> ValueDefinition.forNestedValue elementDotnetType
        let elementDeserializer =
            Deserializer.resolve listSchema.Element elementValue settings
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            arrayValue.Type elementDeserializer createEmpty createFromElementValues

    let createOptionalDeserializer listSchema arrayValue settings =
        createRequiredDeserializer listSchema arrayValue settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = Array1dConverter(Array1dConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if not (isArray1dType sourceValue.Type)
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer sourceValue settings)
                else Option.Some (createRequiredSerializer sourceValue settings)

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if not (isArray1dType targetValue.Type)
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
