namespace Parquet.FSharp.Tests.ByteArray

open Parquet.FSharp
open Swensen.Unquote
open System
open Xunit

module ``byte array deserialized from byte array`` =
    type Input = { Field1: byte[] }
    type Output = { Field1: byte[] }

    [<Fact>]
    let ``null byte array`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for type '{typeof<byte[]>.FullName}'"
                    + " which is not treated as nullable by default" @>)

    [<Fact>]
    let ``empty byte array`` () =
        let value = Array.empty<byte>
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``non-null byte array`` () =
        let value = Convert.FromBase64String("ry9qlnORkUafqDqQW0fyaQ==")
        let inputRecords = [ { Input.Field1 = value }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``byte array option deserialized from byte array`` =
    type Input = { Field1: byte[] }
    type Output = { Field1: byte[] option }

    [<Fact>]
    let ``null byte array`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``empty byte array`` () =
        let value = Array.empty<byte>
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

    [<Fact>]
    let ``non-null byte array`` () =
        let value = Convert.FromBase64String("y01V4KpIHE2QCs9SMvxDbg==")
        let inputRecords = [ { Input.Field1 = value }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
