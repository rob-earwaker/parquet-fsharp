namespace Parquet.FSharp.Tests.Int8

open Parquet.FSharp
open Swensen.Unquote
open System
open Xunit

module ``int8 deserialized from int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int8 }

    [<Fact>]
    let ``zero value`` () =
        let value = 0y
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``min value`` () =
        let value = -128y
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``max value`` () =
        let value = 127y
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``int8 deserialized from int8 option`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int8 }

    [<Fact>]
    let ``none value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for non-nullable"
                    + $" type '{typeof<int8>.FullName}'" @>)

    [<Fact>]
    let ``zero value`` () =
        let value = 0y
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``min value`` () =
        let value = -128y
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``max value`` () =
        let value = 127y
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``int8 deserialized from nullable int8`` =
    type Input = { Field1: Nullable<int8> }
    type Output = { Field1: int8 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for non-nullable"
                    + $" type '{typeof<int8>.FullName}'" @>)

    [<Fact>]
    let ``zero value`` () =
        let value = 0y
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``min value`` () =
        let value = -128y
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Fact>]
    let ``max value`` () =
        let value = 127y
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
