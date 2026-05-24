namespace rec Parquet.FSharp

open System
open System.Linq.Expressions

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Serializer =
    let atomic schema dotnetType dataDotnetType getDataValue =
        let schema =
            let isOptional = false
            ValueSchema.create isOptional schema
        Serializer.Atomic {
            Schema = schema
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            GetDataValue = getDataValue }

    let record dotnetType (fieldSerializers: FieldSerializer[]) =
        let schema =
            let isOptional = false
            let valueType =
                fieldSerializers
                |> Array.map (fun fieldSerailizer -> fieldSerailizer.Schema)
                |> ValueTypeSchema.record
            ValueSchema.create isOptional valueType
        Serializer.Record {
            Schema = schema
            DotnetType = dotnetType
            FieldSerializers = fieldSerializers }

    let list dotnetType (elementSerializer: Serializer) getEnumerator =
        let schema =
            let isOptional = false
            let valueType = ValueTypeSchema.list elementSerializer.Schema
            ValueSchema.create isOptional valueType
        Serializer.List {
            Schema = schema
            DotnetType = dotnetType
            ElementSerializer = elementSerializer
            GetEnumerator = getEnumerator }

    let optional dotnetType (valueSerializer: Serializer) isNull getValue =
        Serializer.Optional {
            Schema = valueSerializer.Schema.MakeOptional()
            DotnetType = dotnetType
            ValueSerializer = valueSerializer
            IsNull = isNull
            GetValue = getValue }

    let wrapAs dotnetType (serializer: Serializer) unwrapValue =
        // Modify an existing serializer such that it instead serializes a
        // wrapper type, providing an expression builder that converts from the
        // wrapper type into the wrapped type.
        match serializer with
        | Serializer.Atomic atomicSerializer ->
            let schema = atomicSerializer.Schema.Type
            let dataDotnetType = atomicSerializer.DataDotnetType
            let getDataValue (value: Expression) =
                let unwrappedValue = unwrapValue value
                atomicSerializer.GetDataValue unwrappedValue
            Serializer.atomic schema dotnetType dataDotnetType getDataValue
        | Serializer.List listSerializer ->
            let elementSerializer = listSerializer.ElementSerializer
            let getEnumerator (list: Expression) =
                let unwrappedList = unwrapValue list
                listSerializer.GetEnumerator unwrappedList
            Serializer.list dotnetType elementSerializer getEnumerator
        | Serializer.Record recordSerializer ->
            let fieldSerializers =
                recordSerializer.FieldSerializers
                |> Array.map (fun fieldSerializer ->
                    let name = fieldSerializer.Name
                    let valueSerializer = fieldSerializer.ValueSerializer
                    let getValue (record: Expression) =
                        let unwrappedRecord = unwrapValue record
                        fieldSerializer.GetValue unwrappedRecord
                    FieldSerializer.create name valueSerializer getValue)
            Serializer.record dotnetType fieldSerializers
        | Serializer.Optional optionalSerializer ->
            let valueSerializer = optionalSerializer.ValueSerializer
            let isNull (optional: Expression) =
                let unwrappedOptional = unwrapValue optional
                optionalSerializer.IsNull unwrappedOptional
            let getValue (optional: Expression) =
                let unwrappedOptional = unwrapValue optional
                optionalSerializer.GetValue unwrappedOptional
            Serializer.optional dotnetType valueSerializer isNull getValue

    let throwIfNull (value: Expression) =
        // if isNull value then
        //     raise SerializationException(...)
        Expression.IfThen(
            Expression.IsNull(value),
            Expression.FailWith<SerializationException>(
                "null value encountered during serialization for type"
                + $" '{value.Type.FullName}' for which nulls are not"
                + " allowed by default"))
        :> Expression

    let optionalNonNullableTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let optionalNullableTypeWrapper allowNulls (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        // If nulls are allowed then we check for null to ensure that any null
        // values are written as NULL. If nulls are not allowed then we always
        // return false regardless of whether the value is null or not. This
        // ensures that the value always gets passed through to the value
        // serializer, which should check for null given that this is a nullable
        // type.
        let isNull =
            if allowNulls
            then Expression.IsNull
            else fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    // TODO: Should this live in Settings.fs?
    let resolveWithValueSettings sourceType (valueSettings: ValueSettings) settings =
        match valueSettings.Converter with
        | Option.Some assignedConverter ->
            assignedConverter.TryCreateSerializer(sourceType, settings)
            |> Option.defaultWith (fun () ->
                raise <| SerializationException(
                    $"could not create serializer for type '{sourceType.FullName}'"
                    + $" using assigned converter '{assignedConverter}'"))
        | Option.None ->
            settings.ValueConverters
            |> List.tryPick _.TryCreateSerializer(sourceType, settings)
            |> Option.defaultWith (fun () ->
                // TODO: This will likely end up depending on attributes as well,
                // so probably will want to make the exception more generic to
                // avoid confusion if there is a converter registered to support the
                // specified type.
                raise <| SerializationException(
                    "could not find converter to serialize type"
                    + $" '{sourceType.FullName}'"))

    // TODO: Should this live in Settings.fs?
    let resolve sourceType settings =
        let valueSettings = Settings.resolveForValue sourceType settings
        resolveWithValueSettings sourceType valueSettings settings

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Deserializer =
    let atomic schema dotnetType dataDotnetType createFromDataValue =
        let schema =
            let isOptional = false
            ValueSchema.create isOptional schema
        Deserializer.Atomic {
            Schema = schema
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            CreateFromDataValue = createFromDataValue }

    let record dotnetType (fieldDeserializers: FieldDeserializer[]) createFromFieldValues =
        let schema =
            let isOptional = false
            let valueType =
                fieldDeserializers
                |> Array.map (fun fieldDeserializer -> fieldDeserializer.Schema)
                |> ValueTypeSchema.record
            ValueSchema.create isOptional valueType
        Deserializer.Record {
            Schema = schema
            DotnetType = dotnetType
            FieldDeserializers = fieldDeserializers
            CreateFromFieldValues = createFromFieldValues }

    let list dotnetType (elementDeserializer: Deserializer) createEmpty createFromElementValues =
        let schema =
            let isOptional = false
            let valueType = ValueTypeSchema.list elementDeserializer.Schema
            ValueSchema.create isOptional valueType
        Deserializer.List {
            Schema = schema
            DotnetType = dotnetType
            ElementDeserializer = elementDeserializer
            CreateEmpty = createEmpty
            CreateFromElementValues = createFromElementValues }

    let optional dotnetType (valueDeserializer: Deserializer) createNull createFromValue =
        Deserializer.Optional {
            Schema = valueDeserializer.Schema.MakeOptional()
            DotnetType = dotnetType
            ValueDeserializer = valueDeserializer
            CreateNull = createNull
            CreateFromValue = createFromValue }

    let wrapAs dotnetType (deserializer: Deserializer) wrapValue =
        // Modify an existing deserializer such that it instead deserializes
        // into a wrapper type, providing an expression builder that converts
        // a value from the original type into the wrapper type.
        match deserializer with
        | Deserializer.Atomic atomicDeserializer ->
            let schema = atomicDeserializer.Schema.Type
            let dataDotnetType = atomicDeserializer.DataDotnetType
            let createFromDataValue dataValue =
                let value = atomicDeserializer.CreateFromDataValue dataValue
                wrapValue value
            Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue
        | Deserializer.List listDeserializer ->
            let elementDeserializer = listDeserializer.ElementDeserializer
            let createEmpty = wrapValue listDeserializer.CreateEmpty
            let createFromElementValues elementValues =
                let list = listDeserializer.CreateFromElementValues elementValues
                wrapValue list
            Deserializer.list
                dotnetType elementDeserializer createEmpty createFromElementValues
        | Deserializer.Record recordDeserializer ->
            let fieldDeserializers = recordDeserializer.FieldDeserializers
            let createFromFieldValues fieldValues =
                let record = recordDeserializer.CreateFromFieldValues fieldValues
                wrapValue record
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
        | Deserializer.Optional optionalDeserializer ->
            let valueDeserializer = optionalDeserializer.ValueDeserializer
            let createNull = wrapValue optionalDeserializer.CreateNull
            let createFromValue value =
                let optional = optionalDeserializer.CreateFromValue value
                wrapValue optional
            Deserializer.optional
                dotnetType valueDeserializer createNull createFromValue

    let throwNullValueEncounteredForNonNullableType (dotnetType: Type) =
        Expression.Block(
            Expression.FailWith<SerializationException>(
                "null value encountered during deserialization for"
                + $" non-nullable type '{dotnetType.FullName}'"),
            Expression.Default(dotnetType))
        :> Expression

    let optionalNonNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull = throwNullValueEncounteredForNonNullableType dotnetType
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: Add argument allowNulls?
    let optionalNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    "null value encountered during deserialization for type"
                    + $" '{dotnetType.FullName}' which is not treated as"
                    + " nullable by default"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: Should this live in Settings.fs?
    let resolveWithValueSettings sourceSchema targetType (valueSettings: ValueSettings) settings =
        match valueSettings.Converter with
        | Option.Some assignedConverter ->
            assignedConverter.TryCreateDeserializer(sourceSchema, targetType, settings)
            |> Option.defaultWith (fun () ->
                raise <| SerializationException(
                    $"could not create deserializer from schema '{sourceSchema}'"
                    + $" to type '{targetType.FullName}' using assigned converter"
                    + $" '{assignedConverter}'"))
        | Option.None ->
            settings.ValueConverters
            |> List.tryPick _.TryCreateDeserializer(sourceSchema, targetType, settings)
            |> Option.defaultWith (fun () ->
                // TODO: This will likely end up depending on attributes as well,
                // so probably will want to make the exception more generic to
                // avoid confusion if there is a converter registered to support the
                // specified type.
                raise <| SerializationException(
                    "could not find converter to deserialize from schema"
                    + $" '{sourceSchema}' to type '{targetType.FullName}'"))

    // TODO: Should this live in Settings.fs?
    let resolve sourceSchema targetType settings =
        let valueSettings = Settings.resolveForValue targetType settings
        resolveWithValueSettings sourceSchema targetType valueSettings settings

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSerializer =
    let create name (valueSerializer: Serializer) getValue =
        let schema = FieldSchema.create name valueSerializer.Schema
        { FieldSerializer.Schema = schema
          FieldSerializer.Name = name
          FieldSerializer.ValueSerializer = valueSerializer
          FieldSerializer.GetValue = getValue }

    let ofField (field: FieldInfo) settings =
        let fieldSettings = Settings.resolveForField field.Field settings
        let name = fieldSettings.Name |> Option.defaultValue field.Name
        let valueSerializer =
            Serializer.resolveWithValueSettings
                field.Type fieldSettings.ValueSettings settings
        let getValue = field.GetValue
        create name valueSerializer getValue

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldDeserializer =
    let create name (valueDeserializer: Deserializer) =
        let schema = FieldSchema.create name valueDeserializer.Schema
        { FieldDeserializer.Schema = schema
          FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let tryOfField (recordSchema: RecordTypeSchema) (field: FieldInfo) settings =
        let fieldSettings = Settings.resolveForField field.Field settings
        // Override field name with configured name (if present) before looking
        // for matching field in the schema.
        let name = fieldSettings.Name |> Option.defaultValue field.Name
        recordSchema.Fields
        |> Array.tryFind _.Name.Equals(name)
        |> Option.map (fun fieldSchema ->
            let deserializer =
                Deserializer.resolveWithValueSettings
                    fieldSchema.Value field.Type fieldSettings.ValueSettings settings
            create name deserializer)
