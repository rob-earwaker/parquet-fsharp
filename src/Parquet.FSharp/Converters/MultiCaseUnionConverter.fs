namespace Parquet.FSharp

open System.Linq.Expressions

// Unions that have one or more cases with one or more fields we model as a
// record, with a field to capture the case name and additional fields to hold
// any associated case data.

type internal MultiCaseUnionConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        MultiCaseUnionConverterSettings.Optional = false
        MultiCaseUnionConverterSettings.AllowNull = false }

// TODO: Allow case type field name to be configured
// TODO: Should single-field union cases be inlined?

// TODO: We should allow all union types really, so that enum and
// single field unions can be serialized in this way too.

type internal MultiCaseUnionConverter(converterSettings: MultiCaseUnionConverterSettings) =
    let createCaseSerializer (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) settings =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.Type
        let valueSerializer =
            let dotnetType = unionInfo.Type
            let fieldSerializers =
                unionCase.Fields
                |> Array.map (fun fieldInfo ->
                    FieldSerializer.ofClassField fieldInfo converterSettings.Optional settings)
            Serializer.record dotnetType fieldSerializers
        // The data for this case is NULL if the union tag does not match the
        // tag for this case.
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let createRequiredSerializer (unionInfo: UnionInfo) settings =
        // Unions that have one or more cases with one or more fields can not be
        // represented as a simple string value. Instead, we have to model the
        // union as a record with a field to capture the case name and
        // additional fields to hold any associated case data.
        let dotnetType = unionInfo.Type
        let unionCasesWithFields =
            unionInfo.Cases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        // The case type field holds the case name. Since unions are not
        // nullable there must always be a case name present. We therefore model
        // this as a required string value.
        let typeFieldSerializer =
            let name = "Type"
            let valueSerializer =
                let dotnetType = typeof<string>
                let dataDotnetType = typeof<string>
                let schema = ValueTypeSchema.primitive dataDotnetType
                let getDataValue = id
                Serializer.atomic schema dotnetType dataDotnetType getDataValue
            let getValue (union: Expression) =
                Expression.Block(
                    Serializer.throwIfNull converterSettings.Optional union,
                    unionInfo.GetCaseName union)
                :> Expression
            FieldSerializer.create name valueSerializer getValue
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFieldSerializers =
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if name = typeFieldSerializer.Name then
                    failwith <|
                        $"case name '{typeFieldSerializer.Name}' is not supported"
                        + $" for union type '{dotnetType}'"
                let valueSerializer = createCaseSerializer unionInfo unionCase settings
                let getValue = id
                FieldSerializer.create name valueSerializer getValue)
        let fieldSerializers = Array.append [| typeFieldSerializer |] caseFieldSerializers
        Serializer.record dotnetType fieldSerializers

    let createOptionalSerializer unionInfo settings =
        createRequiredSerializer unionInfo settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let tryCreateCaseDeserializer
        (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) (schema: RecordTypeSchema) settings =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.Type
        let deserializer =
            let dotnetType = unionInfo.Type
            let fieldDeserializers =
                unionCase.Fields
                |> Array.choose (fun fieldInfo ->
                    FieldDeserializer.tryOfField schema fieldInfo settings)
            let createFromFieldValues = unionCase.CreateFromFieldValues
            if fieldDeserializers.Length < unionCase.Fields.Length
            then Option.None
            else Option.Some (Deserializer.record dotnetType fieldDeserializers createFromFieldValues)
        match deserializer with
        | Option.None -> Option.None
        | Option.Some deserializer ->
            // We can't use {Expression.Null} here because union types are not
            // nullable, however they do still have {null} as their default value
            // because they are reference types.
            let createNull = Expression.Default(dotnetType) :> Expression
            let createFromValue = id
            Deserializer.optional
                dotnetType deserializer createNull createFromValue
            |> Option.Some

    let tryCreateRequiredDeserializer
        (recordSchema: RecordTypeSchema) (unionInfo: UnionInfo) settings =
        let dotnetType = unionInfo.Type
        let unionCasesWithFields =
            unionInfo.Cases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        // The case type field holds the case name as a required string.
        let typeFieldDeserializer =
            let name = "Type"
            let valueDeserializer =
                let dotnetType = typeof<string>
                let dataDotnetType = typeof<string>
                let schema = ValueTypeSchema.primitive dataDotnetType
                let createFromDataValue = id
                Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue
            recordSchema.Fields
            |> Array.tryFind (fun fieldSchema ->
                fieldSchema.Name = name
                && fieldSchema.Value = valueDeserializer.Schema)
            |> Option.map (fun _ -> FieldDeserializer.create name valueDeserializer)
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFieldDeserializers =
            unionCasesWithFields
            |> Array.choose (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if typeFieldDeserializer.IsSome && name = typeFieldDeserializer.Value.Name
                then Option.None
                else
                    recordSchema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
                    |> Option.bind (fun fieldSchema ->
                        match fieldSchema.Value.Type with
                        | ValueTypeSchema.Record recordSchema ->
                            tryCreateCaseDeserializer unionInfo unionCase recordSchema settings
                            |> Option.map (FieldDeserializer.create name)
                        | _ -> Option.None))
        if typeFieldDeserializer.IsNone
            || caseFieldDeserializers.Length < unionCasesWithFields.Length
        then Option.None
        else
            let fieldDeserializers =
                Array.append [| typeFieldDeserializer.Value |] caseFieldDeserializers
            let createFromFieldValues (fieldValues: Expression[]) =
                let caseName = Expression.Variable(typeof<string>, "caseName")
                let returnLabel = Expression.Label(dotnetType, "union")
                Expression.Block(
                    [ caseName ],
                    seq<Expression> {
                        yield Expression.Assign(caseName, fieldValues[0])
                        for caseInfo in unionInfo.Cases do
                            yield Expression.IfThen(
                                Expression.Equal(caseName, Expression.Constant(caseInfo.Name)),
                                if caseInfo.Fields.Length = 0
                                then
                                    Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||])
                                    :> Expression
                                else
                                    let caseIndex =
                                        caseFieldDeserializers
                                        |> Array.findIndex (fun field -> field.Name = caseInfo.Name)
                                    let fieldValue = fieldValues[caseIndex + 1]
                                    Expression.IfThenElse(
                                        Expression.IsNull(Expression.Convert(fieldValue, typeof<obj>)),
                                        Expression.FailWith(
                                            $"no field values found for case '{caseInfo.Name}'"
                                            + $" of union type '{dotnetType}'"),
                                        Expression.Return(returnLabel, fieldValue)))
                        yield Expression.FailWith(
                            $"unknown case name for union of type '{dotnetType}'")
                        yield Expression.Label(returnLabel, Expression.Default(returnLabel.Type))
                    })
                :> Expression
            Option.Some (Deserializer.record dotnetType fieldDeserializers createFromFieldValues)

    let tryCreateOptionalDeserializer recordSchema unionInfo settings =
        tryCreateRequiredDeserializer recordSchema unionInfo settings
        |> Option.map (Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull)

    static member val Default = MultiCaseUnionConverter(MultiCaseUnionConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase -> Option.None
                | UnionCategory.MultiCase ->
                    if converterSettings.Optional
                    then Option.Some (createOptionalSerializer unionInfo settings)
                    else Option.Some (createRequiredSerializer unionInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase -> Option.None
                | UnionCategory.MultiCase ->
                    match sourceSchema.Type with
                    | ValueTypeSchema.Record recordSchema ->
                        if sourceSchema.IsOptional && converterSettings.Optional
                        then tryCreateOptionalDeserializer recordSchema unionInfo settings
                        elif not sourceSchema.IsOptional && not converterSettings.Optional
                        then tryCreateRequiredDeserializer recordSchema unionInfo settings
                        else Option.None
                    | _ -> Option.None
            | _ -> Option.None
