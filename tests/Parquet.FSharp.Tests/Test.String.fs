namespace Parquet.FSharp.Tests.String

open Parquet.FSharp
open Swensen.Unquote
open Xunit

module ``string deserialized as string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Fact>]
    let ``empty string`` () =
        let inputRecords = [| { Input.Field1 = "" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = "" } |] @>

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

module ``string deserialized as string option`` =
    type Input = { Field1: string }
    type Output = { Field1: string option }

    [<Fact>]
    let ``empty string`` () =
        let inputRecords = [| { Input.Field1 = "" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some "" } |] @>

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

    [<Fact>]
    let ``null string`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>
