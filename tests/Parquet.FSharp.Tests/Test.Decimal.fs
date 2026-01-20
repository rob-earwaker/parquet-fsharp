namespace Parquet.FSharp.Tests.Decimal

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize decimal`` =
    type Input = { Field1: decimal }
    type Output = { Field1: decimal }

    let Values = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |]
        [| box 100000000000000000000.000000000000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isDecimal 18 38
                Assert.Field.ConvertedType.isDecimal
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize decimal from required decimal`` =
    type Input = { Field1: decimal }
    type Output = { Field1: decimal }

    let Values = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |]
        [| box 100000000000000000000.000000000000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize decimal from optional decimal`` =
    type Input = { Field1: decimal option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |]
        [| box 100000000000000000000.000000000000000000M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize decimal from required int64`` =
    type Input = { Field1: int64 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box Int64.MinValue; (* outputValue *) box -9223372036854775808.0M |]
        [| (* inputValue *) box            -1L; (* outputValue *) box                   -1.0M |]
        [| (* inputValue *) box             0L; (* outputValue *) box                    0.0M |]
        [| (* inputValue *) box             1L; (* outputValue *) box                    1.0M |]
        [| (* inputValue *) box Int64.MaxValue; (* outputValue *) box  9223372036854775807.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional int64`` =
    type Input = { Field1: int64 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box Int64.MinValue; (* outputValue *) box -9223372036854775808.0M |]
        [| (* inputValue *) box            -1L; (* outputValue *) box                   -1.0M |]
        [| (* inputValue *) box             0L; (* outputValue *) box                    0.0M |]
        [| (* inputValue *) box             1L; (* outputValue *) box                    1.0M |]
        [| (* inputValue *) box Int64.MaxValue; (* outputValue *) box  9223372036854775807.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required int32`` =
    type Input = { Field1: int32 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box Int32.MinValue; (* outputValue *) box -2147483648.0M |]
        [| (* inputValue *) box             -1; (* outputValue *) box          -1.0M |]
        [| (* inputValue *) box              0; (* outputValue *) box           0.0M |]
        [| (* inputValue *) box              1; (* outputValue *) box           1.0M |]
        [| (* inputValue *) box Int32.MaxValue; (* outputValue *) box  2147483647.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional int32`` =
    type Input = { Field1: int32 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box Int32.MinValue; (* outputValue *) box -2147483648.0M |]
        [| (* inputValue *) box             -1; (* outputValue *) box          -1.0M |]
        [| (* inputValue *) box              0; (* outputValue *) box           0.0M |]
        [| (* inputValue *) box              1; (* outputValue *) box           1.0M |]
        [| (* inputValue *) box Int32.MaxValue; (* outputValue *) box  2147483647.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box Int16.MinValue; (* outputValue *) box -32768.0M |]
        [| (* inputValue *) box            -1s; (* outputValue *) box     -1.0M |]
        [| (* inputValue *) box             0s; (* outputValue *) box      0.0M |]
        [| (* inputValue *) box             1s; (* outputValue *) box      1.0M |]
        [| (* inputValue *) box Int16.MaxValue; (* outputValue *) box  32767.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box Int16.MinValue; (* outputValue *) box -32768.0M |]
        [| (* inputValue *) box            -1s; (* outputValue *) box     -1.0M |]
        [| (* inputValue *) box             0s; (* outputValue *) box      0.0M |]
        [| (* inputValue *) box             1s; (* outputValue *) box      1.0M |]
        [| (* inputValue *) box Int16.MaxValue; (* outputValue *) box  32767.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box SByte.MinValue; (* outputValue *) box -128.0M |]
        [| (* inputValue *) box            -1y; (* outputValue *) box   -1.0M |]
        [| (* inputValue *) box             0y; (* outputValue *) box    0.0M |]
        [| (* inputValue *) box             1y; (* outputValue *) box    1.0M |]
        [| (* inputValue *) box SByte.MaxValue; (* outputValue *) box  127.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box SByte.MinValue; (* outputValue *) box -128.0M |]
        [| (* inputValue *) box            -1y; (* outputValue *) box   -1.0M |]
        [| (* inputValue *) box             0y; (* outputValue *) box    0.0M |]
        [| (* inputValue *) box             1y; (* outputValue *) box    1.0M |]
        [| (* inputValue *) box SByte.MaxValue; (* outputValue *) box  127.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required uint64`` =
    type Input = { Field1: uint64 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box UInt64.MinValue; (* outputValue *) box                    0.0M |]
        [| (* inputValue *) box             1UL; (* outputValue *) box                    1.0M |]
        [| (* inputValue *) box UInt64.MaxValue; (* outputValue *) box 18446744073709551615.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional uint64`` =
    type Input = { Field1: uint64 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box UInt64.MinValue; (* outputValue *) box                    0.0M |]
        [| (* inputValue *) box             1UL; (* outputValue *) box                    1.0M |]
        [| (* inputValue *) box UInt64.MaxValue; (* outputValue *) box 18446744073709551615.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box UInt32.MinValue; (* outputValue *) box          0.0M |]
        [| (* inputValue *) box              1u; (* outputValue *) box          1.0M |]
        [| (* inputValue *) box UInt32.MaxValue; (* outputValue *) box 4294967295.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional uint32`` =
    type Input = { Field1: uint32 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box UInt32.MinValue; (* outputValue *) box          0.0M |]
        [| (* inputValue *) box              1u; (* outputValue *) box          1.0M |]
        [| (* inputValue *) box UInt32.MaxValue; (* outputValue *) box 4294967295.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box UInt16.MinValue; (* outputValue *) box     0.0M |]
        [| (* inputValue *) box             1us; (* outputValue *) box     1.0M |]
        [| (* inputValue *) box UInt16.MaxValue; (* outputValue *) box 65535.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box UInt16.MinValue; (* outputValue *) box     0.0M |]
        [| (* inputValue *) box             1us; (* outputValue *) box     1.0M |]
        [| (* inputValue *) box UInt16.MaxValue; (* outputValue *) box 65535.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: decimal }

    let Values = [|
        [| (* inputValue *) box Byte.MinValue; (* outputValue *) box   0.0M |]
        [| (* inputValue *) box           1uy; (* outputValue *) box   1.0M |]
        [| (* inputValue *) box Byte.MaxValue; (* outputValue *) box 255.0M |] |]

    [<Theory>]
    [<MemberData(nameof Values)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize decimal from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: decimal }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>.FullName}'" @>)

    let NonNullValues = [|
        [| (* inputValue *) box Byte.MinValue; (* outputValue *) box   0.0M |]
        [| (* inputValue *) box           1uy; (* outputValue *) box   1.0M |]
        [| (* inputValue *) box Byte.MaxValue; (* outputValue *) box 255.0M |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValues)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
