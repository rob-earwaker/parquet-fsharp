namespace Parquet.FSharp.Tests

open Parquet
open Parquet.Meta
open System.IO

type Field = {
    Schema: SchemaElement
    Children: Field[] }

type Schema = {
    Fields: Field[] }

module ParquetFile =
    let readSchema (bytes: byte[]) =
        let stream = new MemoryStream(bytes)
        use fileReader = ParquetReader.CreateAsync(stream).Result
        let mutable nextIndex = 0
        let readNextElement () =
            let schemaElement = fileReader.Metadata.Schema[nextIndex]
            nextIndex <- nextIndex + 1
            schemaElement
        let rec getGroupFields numChildren =
            Array.init numChildren (fun _ ->
                let schemaElement = readNextElement ()
                let children =
                    if schemaElement.NumChildren.HasValue
                    then getGroupFields schemaElement.NumChildren.Value
                    else [||]
                { Field.Schema = schemaElement
                  Field.Children = children })
        let rootElement = readNextElement ()
        if not rootElement.NumChildren.HasValue then
            failwith "expected root schema element to have children"
        let rootFields = getGroupFields rootElement.NumChildren.Value
        { Schema.Fields = rootFields }
