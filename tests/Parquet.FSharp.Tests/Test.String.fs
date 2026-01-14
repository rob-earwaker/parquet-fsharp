namespace Parquet.FSharp.Tests.String

open Parquet.FSharp
open Swensen.Unquote
open Xunit

module ``string deserialized from string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Fact>]
    let ``null string`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for type '{typeof<string>.FullName}'"
                    + " which is not treated as nullable by default" @>)

    [<Fact>]
    let ``empty string`` () =
        let value = ""
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Theory>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null string`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``string option deserialized from string`` =
    type Input = { Field1: string }
    type Output = { Field1: string option }

    [<Fact>]
    let ``null string`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``empty string`` () =
        let value = ""
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

    [<Theory>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null string`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
