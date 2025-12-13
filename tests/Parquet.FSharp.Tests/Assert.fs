module rec Parquet.FSharp.Tests.Assert

open FSharp.Reflection
open System
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

let private buildArray1dEqualityAssertion (dotnetType: Type) =
    let elementDotnetType = dotnetType.GetElementType()
    let assertElementsEqual = buildEqualityAssertion elementDotnetType
    let array1dEqual (expected: Array) (actual: Array) =
        Assert.Equal(expected.Length, actual.Length)
        for index in [ 0 .. expected.Length - 1 ] do
            let expectedElement = expected.GetValue(index)
            let actualElement = actual.GetValue(index)
            assertElementsEqual expectedElement actualElement
    buildTypedEqualityAssertion array1dEqual

let private buildArrayEqualityAssertion (dotnetType: Type) =
    if dotnetType.GetArrayRank() = 1
    then buildArray1dEqualityAssertion dotnetType
    else failwith "multi-dimensional arrays are not supported"

let private buildRecordEqualityAssertion (dotnetType: Type) =
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

let private buildEqualityAssertion (dotnetType: Type) =
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
    elif FSharpType.IsRecord(dotnetType) then buildRecordEqualityAssertion dotnetType
    else failwith  $"unsupported type '{dotnetType.FullName}'"

let structurallyEqual<'Value> (expected: 'Value) (actual: 'Value) =
    let assertEqual = buildEqualityAssertion typeof<'Value>
    assertEqual (box expected) (box actual)
