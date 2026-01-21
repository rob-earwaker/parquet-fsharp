module rec Parquet.FSharp.Tests.Assert

open FSharp.Reflection
open Parquet.Meta
open Swensen.Unquote
open System
open System.Collections
open System.Collections.Generic
open System.Reflection
open Xunit

let private objEqual (expected: obj) (actual: obj) =
    Assert.Equal(expected, actual)

let private float32Equal (expected: float32) (actual: float32) =
    if Single.IsNaN(expected)
    then Assert.True(Single.IsNaN(actual))
    else Assert.objEqual expected actual

let private floatEqual (expected: float) (actual: float) =
    if Double.IsNaN(expected)
    then Assert.True(Double.IsNaN(actual))
    else Assert.objEqual expected actual

let private buildTypedEqualityAssertion<'Value> assertEqual =
    fun (expected: obj) (actual: obj) ->
        let expected = expected :?> 'Value
        let actual = actual :?> 'Value
        assertEqual expected actual

let private buildEnumerableEqualityAssertion (elementDotnetType: Type) =
    let assertElementsEqual = buildEqualityAssertion elementDotnetType
    let enumerableEqual (expected: IEnumerable) (actual: IEnumerable) =
        let expected = Seq.cast<obj> expected |> Array.ofSeq
        let actual = Seq.cast<obj> actual |> Array.ofSeq
        Assert.Equal(expected.Length, actual.Length)
        for expectedElement, actualElement in Array.zip expected actual do
            assertElementsEqual expectedElement actualElement
    buildTypedEqualityAssertion enumerableEqual

let private buildArray1dEqualityAssertion (dotnetType: Type) =
    let elementDotnetType = dotnetType.GetElementType()
    buildEnumerableEqualityAssertion elementDotnetType

let private buildArrayEqualityAssertion (dotnetType: Type) =
    if dotnetType.GetArrayRank() = 1
    then buildArray1dEqualityAssertion dotnetType
    else failwith "multi-dimensional arrays are not supported"

let private buildFSharpRecordEqualityAssertion (dotnetType: Type) =
    let fieldEqualityAssertions =
        FSharpType.GetRecordFields(dotnetType)
        |> Array.map (fun field -> buildEqualityAssertion field.PropertyType)
    let getFieldValues = FSharpValue.PreComputeRecordReader(dotnetType)
    fun (expected: obj) (actual: obj) ->
        let expectedFieldValues = getFieldValues expected
        let actualFieldValues = getFieldValues actual
        for index in [ 0 .. fieldEqualityAssertions.Length - 1 ] do
            let fieldsEqual = fieldEqualityAssertions[index]
            let expectedFieldValue = expectedFieldValues[index]
            let actualFieldValue = actualFieldValues[index]
            fieldsEqual expectedFieldValue actualFieldValue

let private buildUnionEqualityAssertion (dotnetType: Type) =
    let unionCases = FSharpType.GetUnionCases(dotnetType)
    let unionFieldEqualityAssertions =
        unionCases
        |> Array.map (fun unionCase ->
            unionCase.GetFields()
            |> Array.map (fun field -> buildEqualityAssertion field.PropertyType))
    let getTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
    let unionFieldGetters = unionCases |> Array.map FSharpValue.PreComputeUnionReader
    fun (expected: obj) (actual: obj) ->
        let expectedTag = getTag expected
        let actualTag = getTag actual
        Assert.Equal(expectedTag, actualTag)
        let unionCaseIndex =
            unionCases
            |> Array.findIndex (fun unionCase -> unionCase.Tag = expectedTag)
        let getFieldValues = unionFieldGetters[unionCaseIndex]
        let fieldEqualityAssertions = unionFieldEqualityAssertions[unionCaseIndex]
        let expectedFieldValues = getFieldValues expected
        let actualFieldValues = getFieldValues actual
        for index in [ 0 .. fieldEqualityAssertions.Length - 1 ] do
            let fieldsEqual = fieldEqualityAssertions[index]
            let expectedFieldValue = expectedFieldValues[index]
            let actualFieldValue = actualFieldValues[index]
            fieldsEqual expectedFieldValue actualFieldValue

let private buildClassEqualityAssertion (dotnetType: Type) =
    let properties =
        dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
    let propertyEqualityAssertions =
        properties
        |> Array.map (fun property -> buildEqualityAssertion property.PropertyType)
    fun (expected: obj) (actual: obj) ->
        for index in [ 0 .. properties.Length - 1 ] do
            let property = properties[index]
            let propertiesEqual = propertyEqualityAssertions[index]
            let expectedPropertyValue = property.GetValue(expected)
            let actualPropertyValue = property.GetValue(actual)
            propertiesEqual expectedPropertyValue actualPropertyValue

let private buildGenericListEqualityAssertion (dotnetType: Type) =
    let elementDotnetType = dotnetType.GetGenericArguments()[0]
    buildEnumerableEqualityAssertion elementDotnetType

let private buildFSharpListEqualityAssertion (dotnetType: Type) =
    let elementDotnetType = dotnetType.GetGenericArguments()[0]
    buildEnumerableEqualityAssertion elementDotnetType

let private buildGenericTypeEqualityAssertion (dotnetType: Type) =
    let genericTypeDefinition = dotnetType.GetGenericTypeDefinition()
    if genericTypeDefinition = typedefof<ResizeArray<_>>
    then buildGenericListEqualityAssertion dotnetType
    elif genericTypeDefinition = typedefof<list<_>>
    then buildFSharpListEqualityAssertion dotnetType
    elif genericTypeDefinition = typedefof<Nullable<_>>
    then Assert.objEqual
    elif genericTypeDefinition = typedefof<option<_>>
    then buildUnionEqualityAssertion dotnetType
    else failwith $"unsupported type '{dotnetType.FullName}'"

let private EqualityAssertionCache = Dictionary<Type, obj -> obj -> unit>()

let private tryGetCachedEqualityAssertion dotnetType =
    lock EqualityAssertionCache (fun () ->
        match EqualityAssertionCache.TryGetValue(dotnetType) with
        | false, _ -> Option.None
        | true, equalityAssertion -> Option.Some equalityAssertion)

let private addEqualityAssertionToCache dotnetType equalityAssertion =
    lock EqualityAssertionCache (fun () ->
        EqualityAssertionCache[dotnetType] <- equalityAssertion)

let private buildEqualityAssertion (dotnetType: Type) =
    match tryGetCachedEqualityAssertion dotnetType with
    | Option.Some equalityAssertion -> equalityAssertion
    | Option.None ->
        let equalityAssertion =
            if dotnetType = typeof<bool> then Assert.objEqual
            elif dotnetType = typeof<int8> then Assert.objEqual
            elif dotnetType = typeof<int16> then Assert.objEqual
            elif dotnetType = typeof<int32> then Assert.objEqual
            elif dotnetType = typeof<int64> then Assert.objEqual
            elif dotnetType = typeof<uint8> then Assert.objEqual
            elif dotnetType = typeof<uint16> then Assert.objEqual
            elif dotnetType = typeof<uint32> then Assert.objEqual
            elif dotnetType = typeof<uint64> then Assert.objEqual
            elif dotnetType = typeof<float32> then buildTypedEqualityAssertion Assert.float32Equal
            elif dotnetType = typeof<float> then buildTypedEqualityAssertion Assert.floatEqual
            elif dotnetType = typeof<decimal> then Assert.objEqual
            elif dotnetType = typeof<string> then Assert.objEqual
            elif dotnetType = typeof<Guid> then Assert.objEqual
            elif dotnetType = typeof<DateTime> then Assert.objEqual
            elif dotnetType = typeof<DateTimeOffset> then Assert.objEqual
            elif dotnetType.IsArray then buildArrayEqualityAssertion dotnetType
            elif FSharpType.IsRecord(dotnetType) then buildFSharpRecordEqualityAssertion dotnetType
            elif dotnetType.IsGenericType then buildGenericTypeEqualityAssertion dotnetType
            elif FSharpType.IsUnion(dotnetType) then buildUnionEqualityAssertion dotnetType
            elif dotnetType.IsClass then buildClassEqualityAssertion dotnetType
            else failwith  $"unsupported type '{dotnetType.FullName}'"
        addEqualityAssertionToCache dotnetType equalityAssertion
        equalityAssertion

let equal<'Value> (expected: 'Value) (actual: 'Value) =
    let assertEqual = buildEqualityAssertion typeof<'Value>
    assertEqual (box expected) (box actual)

let arrayLengthEqual (expected: 'Item1[]) (actual: 'Item2[]) =
    Assert.Equal(expected.Length, actual.Length)

let failWith message =
    Assert.Fail(message)

let float64RelativeEqual (expected: float) (actual: float) precision =
    let absActual = Math.Abs(expected)
    let absExpected = Math.Abs(actual)
    let absDiff = Math.Abs(expected - actual)
    test <@ expected = actual || absDiff / (absActual + absExpected) < precision @>

let schema (schema: Schema) fieldAssertions =
    test <@ schema.Fields.Length = Seq.length fieldAssertions @>
    for field, assertField in Seq.zip schema.Fields fieldAssertions do
        assertField field

let field assertions (field: Field) =
    for assertField in assertions do
        assertField field

module Field =
    let nameEquals name (field: Field) =
        test <@ field.Schema.Name = name @>

    let private repetitionIs repetition (field: Field) =
        test <@ field.Schema.RepetitionType = Nullable(repetition) @>

    let isRequired = repetitionIs FieldRepetitionType.REQUIRED
    let isOptional = repetitionIs FieldRepetitionType.OPTIONAL
    let isRepeated = repetitionIs FieldRepetitionType.REPEATED

    module Type =
        let private is type' (field: Field) =
            test <@ field.Schema.Type = Nullable(type') @>

        let hasNoValue (field: Field) =
            test <@ field.Schema.Type = Nullable() @>

        let isBool = is Type.BOOLEAN
        let isInt32 = is Type.INT32
        let isInt64 = is Type.INT64
        let isInt96 = is Type.INT96
        let isFloat32 = is Type.FLOAT
        let isFloat64 = is Type.DOUBLE
        let isByteArray = is Type.BYTE_ARRAY

        let isFixedLengthByteArray length field =
            is Type.FIXED_LEN_BYTE_ARRAY field
            test <@ field.Schema.TypeLength = Nullable(length) @>

    module LogicalType =
        let hasNoValue (field: Field) =
            test <@ isNull field.Schema.LogicalType @>

        let isUuid (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.UUID) @>

        let isString (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.STRING) @>

        let isList (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.LIST) @>

        let isInteger (bitWidth: int) isSigned (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.INTEGER) @>
            test <@ field.Schema.LogicalType.INTEGER.BitWidth = sbyte bitWidth @>
            test <@ field.Schema.LogicalType.INTEGER.IsSigned = isSigned @>

        let isDecimal scale precision (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.DECIMAL) @>
            test <@ field.Schema.LogicalType.DECIMAL.Scale = scale @>
            test <@ field.Schema.LogicalType.DECIMAL.Precision = precision @>
            test <@ field.Schema.Scale = Nullable(scale) @>
            test <@ field.Schema.Precision = Nullable(precision) @>

        let isTimestamp isAdjustedToUtc unit (field: Field) =
            test <@ not (isNull field.Schema.LogicalType) @>
            test <@ not (isNull field.Schema.LogicalType.TIMESTAMP) @>
            test <@ not (isNull field.Schema.LogicalType.TIMESTAMP.Unit) @>
            test <@ field.Schema.LogicalType.TIMESTAMP.IsAdjustedToUTC = isAdjustedToUtc @>
            match unit with
            | "milliseconds" ->
                test <@ not (isNull field.Schema.LogicalType.TIMESTAMP.Unit.MILLIS) @>
            | "microseconds" ->
                test <@ not (isNull field.Schema.LogicalType.TIMESTAMP.Unit.MICROS) @>
            | "nanoseconds" ->
                test <@ not (isNull field.Schema.LogicalType.TIMESTAMP.Unit.NANOS) @>
            | _ ->
                failwith $"invalid unit '{unit}'"

    module ConvertedType =
        let private is convertedType (field: Field) =
            test <@ field.Schema.ConvertedType = Nullable(convertedType) @>

        let hasNoValue (field: Field) =
            test <@ field.Schema.ConvertedType = Nullable() @>

        let isUtf8 = is ConvertedType.UTF8
        let isInt8 = is ConvertedType.INT_8
        let isInt16 = is ConvertedType.INT_16
        let isInt32 = is ConvertedType.INT_32
        let isInt64 = is ConvertedType.INT_64
        let isUInt8 = is ConvertedType.UINT_8
        let isUInt16 = is ConvertedType.UINT_16
        let isUInt32 = is ConvertedType.UINT_32
        let isUInt64 = is ConvertedType.UINT_64
        let isDecimal = is ConvertedType.DECIMAL
        let isList = is ConvertedType.LIST

    let hasNoChildren (field: Field) =
        test <@ field.Children = [||] @>

    let children childAssertions (field: Field) =
        test <@ field.Children.Length = Seq.length childAssertions @>
        for childField, assertChild in Seq.zip field.Children childAssertions do
            assertChild childField

    let child childAssertions (field: Field) =
        children [ Assert.field childAssertions ] field
