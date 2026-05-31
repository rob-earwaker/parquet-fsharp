namespace Parquet.FSharp

open System
open System.Collections.Generic
open System.Linq.Expressions

type internal ResizeArrayConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        ResizeArrayConverterSettings.Optional = false
        ResizeArrayConverterSettings.AllowNull = false }

type internal ResizeArrayConverter(converterSettings: ResizeArrayConverterSettings) =
    let isResizeArrayType = DotnetType.isGenericType<ResizeArray<_>>

    let createSerializer (arrayValue: ValueDefinition) settings =
        let elementDotnetType = arrayValue.Type.GetGenericArguments()[0]
        let elementValue = arrayValue |> ValueDefinition.forNestedValue elementDotnetType
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
        Serializer.list arrayValue.Type elementSerializer getEnumerator

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (listSchema: ListTypeSchema) (arrayValue: ValueDefinition) settings =
        let elementDotnetType = arrayValue.Type.GetGenericArguments()[0]
        let elementValue = arrayValue |> ValueDefinition.forNestedValue elementDotnetType
        let elementDeserializer =
            Deserializer.resolve listSchema.Element elementValue settings
        let createEmpty = Expression.New(arrayValue.Type)
        let createFromElementValues = id
        Deserializer.list
            arrayValue.Type elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer listSchema arrayValue settings =
        createRequiredDeserializer listSchema arrayValue settings
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = ResizeArrayConverter(ResizeArrayConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if isResizeArrayType sourceValue.Type
            then Option.Some (createSerializer sourceValue settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if not (isResizeArrayType targetValue.Type)
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
