namespace rec Parquet.FSharp

open System
open System.Linq.Expressions
open System.Reflection

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldDefinition =
    let [<Literal>] private RootName = "$root"

    let create name valueType attributes =
        { FieldDefinition.Name = name
          FieldDefinition.ValueType = valueType
          FieldDefinition.Attributes = attributes }

    let forRoot (recordType: Type) =
        let attributes = [||]
        create RootName recordType attributes

    let ofProperty (property: PropertyInfo) =
        let attributes = property.GetCustomAttributes() |> Array.ofSeq
        create property.Name property.PropertyType attributes

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal ValueDefinition =
    let create field nestingLevel valueType attributes =
        { ValueDefinition.Field = field
          ValueDefinition.NestingLevel = nestingLevel
          ValueDefinition.Type = valueType
          ValueDefinition.Attributes = attributes }

    let ofField (field: FieldDefinition) =
        let nestingLevel = 0
        let attributes = field.ValueType.GetCustomAttributes() |> Array.ofSeq
        create field nestingLevel field.ValueType attributes

    let forNestedValue (nestedValueType: Type) (value: ValueDefinition) =
        let nestingLevel = value.NestingLevel + 1
        let attributes = nestedValueType.GetCustomAttributes() |> Array.ofSeq
        create value.Field nestingLevel nestedValueType attributes

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

    // TODO: This might end up only being used by optional values. Can we simplify?
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

    let throwIfNull allowNull (value: Expression) =
        let exnMessage =
            // TODO: These messages are used for optional wrappers too (see below function)
            // but in the case optional=false and allowNull=false we kinda want the top
            // message (or even a combination of the top and bottom) since setting allowNull=true
            // on its own will not resolve null values.
            if allowNull
            then
                "null value encountered during serialization for type"
                + $" '{value.Type}' which is not optional by default"
            else
                "null value encountered during serialization for type"
                + $" '{value.Type}' for which nulls are not allowed by default"
        Expression.IfThen(
            Expression.IsNull(value),
            Expression.FailWith<SerializationException>(exnMessage))
        :> Expression

    let optionalNonNullableTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let optionalNullableTypeWrapper allowNull (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        // If nulls are allowed then we check for null to ensure that any null
        // values are written as NULL. If nulls are not allowed then we always
        // return false regardless of whether the value is null or not. This
        // ensures that the value always gets passed through to the value
        // serializer, which should check for null given that this is a nullable
        // type.
        let isNull =
            if allowNull
            then Expression.IsNull
            else fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    // TODO: Should this live in Settings.fs?
    // TODO: Exceptions could be improved with field path and nesting level info!
    let resolve sourceValue settings =
        let valueSettings = Settings.resolveForValue sourceValue settings
        match valueSettings.Converter with
        | Option.Some assignedConverter ->
            assignedConverter.TryCreateSerializer(sourceValue, settings)
            |> Option.defaultWith (fun () ->
                raise <| SerializationException(
                    $"could not create serializer for type '{sourceValue.Type}'"
                    + $" using assigned converter '{assignedConverter}'"))
        | Option.None ->
            settings.ValueConverters
            |> List.tryPick _.TryCreateSerializer(sourceValue, settings)
            |> Option.defaultWith (fun () ->
                // TODO: This will likely end up depending on attributes as well,
                // so probably will want to make the exception more generic to
                // avoid confusion if there is a converter registered to support the
                // specified type.
                raise <| SerializationException(
                    $"could not find converter to serialize type '{sourceValue.Type}'"))

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

    // TODO: This might end up only being used by optional values. Can we simplify?
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

    let optionalNonNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{dotnetType}'"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    let optionalNullableTypeWrapper allowNull (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            if allowNull
            then Expression.Null(dotnetType)
            else
                Expression.Block(
                    Expression.FailWith<SerializationException>(
                        "null value encountered during deserialization for type"
                        + $" '{dotnetType}' for which nulls are not allowed"
                        + " by default"),
                    Expression.Default(dotnetType))
                :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: Should this live in Settings.fs?
    let resolve sourceSchema targetValue settings =
        let valueSettings = Settings.resolveForValue targetValue settings
        match valueSettings.Converter with
        | Option.Some assignedConverter ->
            assignedConverter.TryCreateDeserializer(sourceSchema, targetValue, settings)
            |> Option.defaultWith (fun () ->
                raise <| SerializationException(
                    $"could not create deserializer from schema '{sourceSchema}'"
                    + $" to type '{targetValue.Type}' using assigned converter"
                    + $" '{assignedConverter}'"))
        | Option.None ->
            settings.ValueConverters
            |> List.tryPick _.TryCreateDeserializer(sourceSchema, targetValue, settings)
            |> Option.defaultWith (fun () ->
                // TODO: This will likely end up depending on attributes as well,
                // so probably will want to make the exception more generic to
                // avoid confusion if there is a converter registered to support the
                // specified type.
                raise <| SerializationException(
                    "could not find converter to deserialize from schema"
                    + $" '{sourceSchema}' to type '{targetValue.Type}'"))

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSerializer =
    let create name (valueSerializer: Serializer) getValue =
        let schema = FieldSchema.create name valueSerializer.Schema
        { FieldSerializer.Schema = schema
          FieldSerializer.Name = name
          FieldSerializer.ValueSerializer = valueSerializer
          FieldSerializer.GetValue = getValue }

    let ofField (fieldInfo: Parquet.FSharp.FieldInfo) settings =
        // TODO: When we resolve value settings at the field level we only see
        // attributes applied to the field and the top-level value type, we don't
        // see attributes applied to nested types and can currently only configure
        // these if the field is annotated with a {ParquetNestedValueAttribute}.
        // How can we ensure nested type attributes are picked up as part of this
        // settings resolution process? It's difficult to do it in
        // Settings.resolveForField or Settings.resolveForValue since we would
        // have to recurse down the type heirarchy, identifying nesting and =
        // pulling out attributes as we go. We do this type recursion already as
        // part of the serializer resolution, so makes sense to build it in as
        // part of that somehow. This probably means resolving value settings
        // based on the type at the current nesting level even if we've already
        // resolved value settings for the field. However, we generally want
        // field attributes to override type attributes so this needs some thought!
        let fieldDefinition = FieldDefinition.ofProperty fieldInfo.Property
        let fieldSettings = Settings.resolveForField fieldDefinition settings
        let name = fieldSettings.Name |> Option.defaultValue fieldInfo.Name
        let valueDefinition = ValueDefinition.ofField fieldDefinition
        let valueSerializer = Serializer.resolve valueDefinition settings
        let getValue = fieldInfo.GetValue
        create name valueSerializer getValue

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldDeserializer =
    let create name (valueDeserializer: Deserializer) =
        let schema = FieldSchema.create name valueDeserializer.Schema
        { FieldDeserializer.Schema = schema
          FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let tryOfField (recordSchema: RecordTypeSchema) (fieldInfo: Parquet.FSharp.FieldInfo) settings =
        let fieldDefinition = FieldDefinition.ofProperty fieldInfo.Property
        let fieldSettings = Settings.resolveForField fieldDefinition settings
        // Override field name with configured name (if present) before looking
        // for matching field in the schema.
        let name = fieldSettings.Name |> Option.defaultValue fieldInfo.Name
        recordSchema.Fields
        |> Array.tryFind _.Name.Equals(name)
        |> Option.map (fun fieldSchema ->
            let valueDefinition = ValueDefinition.ofField fieldDefinition
            let deserializer = Deserializer.resolve fieldSchema.Value valueDefinition settings
            create name deserializer)
