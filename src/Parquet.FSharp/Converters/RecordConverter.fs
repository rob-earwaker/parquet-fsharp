namespace Parquet.FSharp

type internal RecordConverter private () =
    let createSerializer (recordInfo: RecordInfo) settings =
        let fieldSerializers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo settings)
        Serializer.record recordInfo.Type fieldSerializers

    let tryCreateRequiredDeserializer
        (recordSchema: RecordTypeSchema) (recordInfo: RecordInfo) settings =
        let fieldDeserializers =
            recordInfo.Fields
            |> Array.choose (fun fieldInfo ->
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = fieldInfo.Name)
                |> Option.map (fun fieldSchema ->
                    FieldDeserializer.ofField fieldSchema.Value fieldInfo settings))
        if fieldDeserializers.Length < recordInfo.Fields.Length
        then Option.None
        else
            Deserializer.record
                recordInfo.Type fieldDeserializers recordInfo.CreateFromFieldValues
            |> Option.Some

    let tryCreateOptionalDeserializer recordSchema recordInfo settings =
        tryCreateRequiredDeserializer recordSchema recordInfo settings
        |> Option.map Deserializer.optionalNonNullableTypeWrapper

    static member val Default = RecordConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Record recordInfo ->
                Option.Some (createSerializer recordInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Record recordInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional
                    then tryCreateOptionalDeserializer recordSchema recordInfo settings
                    else tryCreateRequiredDeserializer recordSchema recordInfo settings
                | _ -> Option.None
            | _ -> Option.None
