module rec Parquet.FSharp.Thrift
// TODO: This might move to internal.

open Parquet.Meta

type Schema = {
    Fields: Field[] }

type Field = {
    Name: string
    Repetition: Repetition
    Type: Type }

type Repetition =
    | Required
    | Optional
    | Repeated

type Type =
    | Int8
    | String
    | Timestamp of TimestampType
    | ByteArray
    | Struct of Field[]
    | List of Field[]

type TimestampType = {
    IsAdjustedToUtc: bool
    Unit: TimeUnit }

type TimeUnit =
    | Milliseconds
    | Microseconds
    | Nanoseconds

let schema fields =
    { Schema.Fields = Array.ofSeq fields }

let required (fieldBuilder: Repetition -> 'FieldBuilder) =
    fieldBuilder Repetition.Required

let optional (fieldBuilder: Repetition -> 'FieldBuilder) =
    fieldBuilder Repetition.Optional

let repeated (fieldBuilder: Repetition -> 'FieldBuilder) =
    fieldBuilder Repetition.Repeated

let int8 repetition name =
    { Field.Name = name
      Field.Repetition = repetition
      Field.Type = Type.Int8 }

let string repetition name =
    { Field.Name = name
      Field.Repetition = repetition
      Field.Type = Type.String }

let record repetition name fields =
    { Field.Name = name
      Field.Repetition = repetition
      Field.Type = Type.List (Array.ofSeq fields) }

let list repetition name fields =
    { Field.Name = name
      Field.Repetition = repetition
      Field.Type = Type.List (Array.ofSeq fields) }

let timestamp repetition (isAdjustedToUtc, unit) name =
    { Field.Name = name
      Field.Repetition = repetition
      Field.Type = Type.Timestamp {
          IsAdjustedToUtc = isAdjustedToUtc
          Unit = unit } }

let utc = true
let local = false

let milliseconds = TimeUnit.Milliseconds
let microseconds = TimeUnit.Microseconds
let nanoseconds = TimeUnit.Nanoseconds

let internal buildSchema (fileMetaData: FileMetaData) =
    let schema =
        schema [
            optional string "Field1"
            required timestamp (local, milliseconds) "Field2"
            optional list "Field3" [
                repeated record "list" [
                    required int8 "element" ] ] ]
    let mutable nextIndex = 0
    let readNextElement () =
        let schemaElement = fileMetaData.Schema[nextIndex]
        nextIndex <- nextIndex + 1
        schemaElement
    let rec getGroupFields numChildren =
        Array.init numChildren (fun _ ->
            let schemaElement = readNextElement ()
            let type' =
                if schemaElement.NumChildren.HasValue then
                    let fields = getGroupFields schemaElement.NumChildren.Value
                    if isNull schemaElement.LogicalType
                    then Type.Struct fields
                    elif not (isNull schemaElement.LogicalType.LIST)
                    then Type.List fields
                    else failwith "unsupported group type"
                else
                    Type.Int8
            let repetition =
                match schemaElement.RepetitionType.Value with
                | FieldRepetitionType.REQUIRED -> Repetition.Required
                | FieldRepetitionType.OPTIONAL -> Repetition.Optional
                | FieldRepetitionType.REPEATED -> Repetition.Repeated
                | value -> failwith $"unsupported repetition type '{value}'"
            { Field.Name = schemaElement.Name
              Field.Repetition = repetition
              Field.Type = type' })
    let rootElement = readNextElement ()
    let rootFields = getGroupFields rootElement.NumChildren.Value
    { Schema.Fields = rootFields }
