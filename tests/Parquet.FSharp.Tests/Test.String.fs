namespace Parquet.FSharp.Tests.String

open Parquet.FSharp
open Swensen.Unquote
open Xunit

module ``non-null string deserialized as string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``test`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``null string deserialized as string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Fact>]
    let ``test`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for type '{typeof<string>.FullName}'"
                    + " which is not treated as nullable by default" @>)

module ``null string deserialized as string option`` =
    type Input = { Field1: string }
    type Output = { Field1: string option }

    [<Fact>]
    let ``test`` () =
        let inputRecords = [ { Input.Field1 = null }]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>
