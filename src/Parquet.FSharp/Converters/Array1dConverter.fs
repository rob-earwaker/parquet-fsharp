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

    let createRequiredSerializer value settings =
        let elementDotnetType = value.Type.GetElementType()
        let elementValue = value |> ValueDefinition.forNestedValue elementDotnetType
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
                Serializer.throwIfNull converterSettings.Optional array,
                Expression.Assign(enumerable, Expression.Convert(array, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list value.Type elementSerializer getEnumerator

    let createOptionalSerializer value settings =
        createRequiredSerializer value settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let createRequiredDeserializer (schema: ListTypeSchema) (value: ValueDefinition) settings =
        let elementDotnetType = value.Type.GetElementType()
        let elementValue = value |> ValueDefinition.forNestedValue elementDotnetType
        let elementDeserializer =
            Deserializer.resolve schema.Element elementValue settings
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            value.Type elementDeserializer createEmpty createFromElementValues

    let createOptionalDeserializer schema value settings =
        createRequiredDeserializer schema value settings
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
                | ValueTypeSchema.List schema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some (createOptionalDeserializer schema targetValue settings)
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some (createRequiredDeserializer schema targetValue settings)
                    else Option.None
                | _ -> Option.None
