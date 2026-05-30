namespace Parquet.FSharp

type internal RecordConverter private () =
    let createSerializer (recordInfo: RecordInfo) settings =
        let fieldSerializers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo settings)
        Serializer.record recordInfo.Type fieldSerializers

    let tryCreateRequiredDeserializer recordSchema (recordInfo: RecordInfo) settings =
        let fieldDeserializers =
            recordInfo.Fields
            |> Array.choose (fun fieldInfo ->
                FieldDeserializer.tryOfField recordSchema fieldInfo settings)
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
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Record recordInfo ->
                Option.Some (createSerializer recordInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Record recordInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional
                    then tryCreateOptionalDeserializer recordSchema recordInfo settings
                    else tryCreateRequiredDeserializer recordSchema recordInfo settings
                | _ -> Option.None
            | _ -> Option.None
