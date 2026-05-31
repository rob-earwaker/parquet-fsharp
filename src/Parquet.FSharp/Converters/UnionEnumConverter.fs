namespace Parquet.FSharp

open System.Linq.Expressions

// Union enums are those in which none of the cases have any fields, which allows them to be
// represented as a simple string value containing the case name. Since a union value shouldn't be
// null and must be one of the possible cases, this string value is not optional by default.

type internal UnionEnumConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        UnionEnumConverterSettings.Optional = false
        UnionEnumConverterSettings.AllowNull = false }

type internal UnionEnumConverter(converterSettings: UnionEnumConverterSettings) =
    let dataDotnetType = typeof<string>

    let createRequiredSerializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.Type
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = unionInfo.GetCaseName
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createOptionalSerializer unionInfo =
        createRequiredSerializer unionInfo
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let createRequiredDeserializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.Type
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue caseName =
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                seq<Expression> {
                    yield! unionInfo.Cases
                        |> Array.map (fun caseInfo ->
                            Expression.IfThen(
                                Expression.Equal(caseName, Expression.Constant(caseInfo.Name)),
                                Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||]))
                            :> Expression)
                    yield Expression.FailWith<SerializationException>(
                        Expression.Constant("encountered invalid case name '"),
                        // TODO: Could detect null values here and print '<null>' instead of ''
                        caseName,
                        Expression.Constant(
                            $"' during deserialization of enum union type '{dotnetType}'"))
                    yield Expression.Label(returnLabel, Expression.Default(dotnetType))
                })
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer unionInfo =
        createRequiredDeserializer unionInfo
        |> Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull

    static member val Default = UnionEnumConverter(UnionEnumConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum ->
                    if converterSettings.Optional
                    then Option.Some (createOptionalSerializer unionInfo)
                    else Option.Some (createRequiredSerializer unionInfo)
                | UnionCategory.SingleCase -> Option.None
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum ->
                    match sourceSchema.Type with
                    // Only support atomic values with the correct type.
                    | ValueTypeSchema.Primitive primitiveSchema
                        when primitiveSchema.DataDotnetType = dataDotnetType ->
                        if sourceSchema.IsOptional && converterSettings.Optional
                        then Option.Some (createOptionalDeserializer unionInfo)
                        elif not sourceSchema.IsOptional && not converterSettings.Optional
                        then Option.Some (createRequiredDeserializer unionInfo)
                        else Option.None
                    | _ -> Option.None
                | UnionCategory.SingleCase -> Option.None
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None
