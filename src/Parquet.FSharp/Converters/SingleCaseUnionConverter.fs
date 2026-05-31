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

// TODO: Make use of settings.
// TODO: Should single-field union cases be inlined?

type internal SingleCaseUnionConverter(converterSettings: SingleCaseUnionConverterSettings) =
    let createSerializer (unionInfo: UnionInfo) settings =
        let dotnetType = unionInfo.Type
        let unionCase = unionInfo.Cases[0]
        let fieldSerializers =
            unionCase.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo settings)
        Serializer.record dotnetType fieldSerializers

    let tryCreateDeserializer
        (sourceSchema: ValueSchema) (unionInfo: UnionInfo) settings =
        match sourceSchema.Type with
        | ValueTypeSchema.Record recordSchema ->
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
                let requiredValueDeserializer =
                    Deserializer.record dotnetType fieldDeserializers createFromFieldValues
                let deserializer =
                    if sourceSchema.IsOptional
                    then requiredValueDeserializer |> Deserializer.optionalNonNullableTypeWrapper
                    else requiredValueDeserializer
                Option.Some deserializer
        | _ -> Option.None

    static member val Default = SingleCaseUnionConverter(SingleCaseUnionConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase -> Option.Some (createSerializer unionInfo settings)
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum -> Option.None
                | UnionCategory.SingleCase ->
                    tryCreateDeserializer sourceSchema unionInfo settings
                | UnionCategory.MultiCase -> Option.None
            | _ -> Option.None
