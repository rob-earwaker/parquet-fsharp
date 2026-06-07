namespace Parquet.FSharp

// Unions with a single case are most likely being used to enable stricter type checking and to
// allow encapsulation of any associated field values. We serialize single case unions as a record
// using the case field names and types.

type internal SingleCaseUnionConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        SingleCaseUnionConverterSettings.Optional = false
        SingleCaseUnionConverterSettings.AllowNull = false }

// TODO: Should single-field union cases be inlined? Not by default but maybe setting?
// Should this be a configuration option on the union case rather than the union itself so we
// can use for multi-case union serialization?

type internal SingleCaseUnionConverter(converterSettings: SingleCaseUnionConverterSettings) =
    let createRequiredSerializer (unionInfo: UnionInfo) settings =
        let dotnetType = unionInfo.Type
        let unionCase = unionInfo.Cases[0]
        let fieldSerializers =
            unionCase.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofClassField fieldInfo converterSettings.Optional settings)
        Serializer.record dotnetType fieldSerializers

    let createOptionalSerializer unionInfo settings =
        createRequiredSerializer unionInfo settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let tryCreateRequiredDeserializer (recordSchema: RecordTypeSchema) (unionInfo: UnionInfo) settings =
        let dotnetType = unionInfo.Type
        let unionCase = unionInfo.Cases[0]
        let fieldDeserializers =
            unionCase.Fields
            |> Array.choose (fun fieldInfo ->
                FieldDeserializer.tryOfField recordSchema fieldInfo settings)
        let createFromFieldValues = unionCase.CreateFromFieldValues
        if fieldDeserializers.Length < unionCase.Fields.Length
        then Option.None
        else
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
            |> Option.Some 

    let tryCreateOptionalDeserializer recordSchema unionInfo settings =
        tryCreateRequiredDeserializer recordSchema unionInfo settings
        |> Option.map (Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull)

    static member val Default = SingleCaseUnionConverter(SingleCaseUnionConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase ->
                    if converterSettings.Optional
                    then Option.Some (createOptionalSerializer unionInfo settings)
                    else Option.Some (createRequiredSerializer unionInfo settings)
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase ->
                    match sourceSchema.Type with
                    | ValueTypeSchema.Record recordSchema ->
                        if sourceSchema.IsOptional && converterSettings.Optional
                        then tryCreateOptionalDeserializer recordSchema unionInfo settings
                        elif not sourceSchema.IsOptional && not converterSettings.Optional
                        then tryCreateRequiredDeserializer recordSchema unionInfo settings
                        else Option.None
                    | _ -> Option.None
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None
