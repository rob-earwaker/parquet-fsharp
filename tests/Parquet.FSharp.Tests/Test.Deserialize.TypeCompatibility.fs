module Parquet.FSharp.Tests.Deserialize.TypeCompatability

open FsCheck.Xunit
open Parquet.FSharp
open Parquet.FSharp.Tests

// TODO: Should we support checked/unchecked conversions and conversions that
// result in a loss of precision? Maybe require an attribute in the unsafe cases.
// Have only added tests for safe cases that do not result in loss of precision:
// https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/numeric-conversions

// TODO: Tests for ability to load list types interchangeably.

let testTypeCompatibility<'Input, 'Output> convert inputRecords =
    let bytes = ParquetSerializer.Serialize<{| Field1: 'Input |}>(inputRecords)
    let outputRecords = ParquetSerializer.Deserialize<{| Field1: 'Output |}>(bytes)
    Assert.arrayLengthEqual inputRecords outputRecords
    for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
        let inputField1 = convert inputRecord.Field1
        let outputField1 = outputRecord.Field1
        Assert.equal inputField1 outputField1

[<Property>]
let ``int16 deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, int16> int16 inputRecords

[<Property>]
let ``int16 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, int16> int16 inputRecords

[<Property>]
let ``int32 deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, int32> int32 inputRecords

[<Property>]
let ``int32 deserialized from int16`` inputRecords =
    testTypeCompatibility<int16, int32> int32 inputRecords

[<Property>]
let ``int32 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, int32> int32 inputRecords

[<Property>]
let ``int32 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, int32> int32 inputRecords

[<Property>]
let ``int64 deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, int64> int64 inputRecords

[<Property>]
let ``int64 deserialized from int16`` inputRecords =
    testTypeCompatibility<int16, int64> int64 inputRecords

[<Property>]
let ``int64 deserialized from int32`` inputRecords =
    testTypeCompatibility<int32, int64> int64 inputRecords

[<Property>]
let ``int64 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, int64> int64 inputRecords

[<Property>]
let ``int64 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, int64> int64 inputRecords

[<Property>]
let ``int64 deserialized from uint32`` inputRecords =
    testTypeCompatibility<uint32, int64> int64 inputRecords

[<Property>]
let ``uint16 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, uint16> uint16 inputRecords

[<Property>]
let ``uint32 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, uint32> uint32 inputRecords

[<Property>]
let ``uint32 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, uint32> uint32 inputRecords

[<Property>]
let ``uint64 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, uint64> uint64 inputRecords

[<Property>]
let ``uint64 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, uint64> uint64 inputRecords

[<Property>]
let ``uint64 deserialized from uint32`` inputRecords =
    testTypeCompatibility<uint32, uint64> uint64 inputRecords

[<Property>]
let ``float32 deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, float32> float32 inputRecords

[<Property>]
let ``float32 deserialized from int16`` inputRecords =
    testTypeCompatibility<int16, float32> float32 inputRecords

[<Property>]
let ``float32 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, float32> float32 inputRecords

[<Property>]
let ``float32 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, float32> float32 inputRecords

[<Property>]
let ``float64 deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, float> float inputRecords

[<Property>]
let ``float64 deserialized from int16`` inputRecords =
    testTypeCompatibility<int16, float> float inputRecords

[<Property>]
let ``float64 deserialized from int32`` inputRecords =
    testTypeCompatibility<int32, float> float inputRecords

[<Property>]
let ``float64 deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, float> float inputRecords

[<Property>]
let ``float64 deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, float> float inputRecords

[<Property>]
let ``float64 deserialized from uint32`` inputRecords =
    testTypeCompatibility<uint32, float> float inputRecords

[<Property>]
let ``float64 deserialized from float32`` inputRecords =
    testTypeCompatibility<float32, float> float inputRecords

[<Property>]
let ``decimal deserialized from int8`` inputRecords =
    testTypeCompatibility<int8, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from int16`` inputRecords =
    testTypeCompatibility<int16, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from int32`` inputRecords =
    testTypeCompatibility<int32, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from int64`` inputRecords =
    testTypeCompatibility<int64, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from uint8`` inputRecords =
    testTypeCompatibility<uint8, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from uint16`` inputRecords =
    testTypeCompatibility<uint16, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from uint32`` inputRecords =
    testTypeCompatibility<uint32, decimal> decimal inputRecords

[<Property>]
let ``decimal deserialized from uint64`` inputRecords =
    testTypeCompatibility<uint64, decimal> decimal inputRecords
