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

    let createSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = Serializer.resolve elementDotnetType settings
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
        Serializer.list dotnetType elementSerializer getEnumerator

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType settings
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType settings =
        createRequiredDeserializer schema dotnetType settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = Array1dConverter(Array1dConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            if isArray1dType sourceType
            then Option.Some (createSerializer sourceType settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isArray1dType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    else Option.Some (createRequiredDeserializer listSchema targetType settings)
                | _ -> Option.None
