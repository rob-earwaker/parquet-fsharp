namespace Parquet.FSharp.Tests.DefaultConverter.ByteArray

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize byte array`` =
    type Input = { Field1: byte[] }
    type Output = { Field1: byte[] }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<byte[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Theory>]
    [<InlineData("")>] // Empty
    [<InlineData("AA==")>] // Single zero value
    [<InlineData("y01V4KpIHE2QCs9SMvxDbg==")>] // Random GUID
    let ``non-null value`` base64 =
        let value = Convert.FromBase64String(base64)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize byte array from required byte array`` =
    type Input = { Field1: byte[] }
    type Output = { Field1: byte[] }

    [<Theory>]
    [<InlineData("")>] // Empty
    [<InlineData("AA==")>] // Single zero value
    [<InlineData("y01V4KpIHE2QCs9SMvxDbg==")>] // Random GUID
    let ``value`` base64 =
        let value = Convert.FromBase64String(base64)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize byte array from optional byte array`` =
    type Input = { Field1: byte[] option }
    type Output = { Field1: byte[] }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<byte[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Theory>]
    [<InlineData("")>] // Empty
    [<InlineData("AA==")>] // Single zero value
    [<InlineData("y01V4KpIHE2QCs9SMvxDbg==")>] // Random GUID
    let ``non-null value`` base64 =
        let value = Convert.FromBase64String(base64)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
