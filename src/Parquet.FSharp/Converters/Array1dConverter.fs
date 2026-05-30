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

    let createSerializer arrayValue settings =
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

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
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

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer listSchema arrayValue settings =
        createRequiredDeserializer listSchema arrayValue settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = Array1dConverter(Array1dConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if isArray1dType sourceValue.Type
            then Option.Some (createSerializer sourceValue settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if not (isArray1dType targetValue.Type)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetValue settings)
                    else Option.Some (createRequiredDeserializer listSchema targetValue settings)
                | _ -> Option.None
