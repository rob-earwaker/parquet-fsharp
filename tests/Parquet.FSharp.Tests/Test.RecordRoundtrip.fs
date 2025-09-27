module Parquet.FSharp.Tests.RecordRoundtrip

open FsCheck.Xunit
open Parquet.FSharp
open Swensen.Unquote
open System
open System.IO

let testRecordRoundtrip<'Record when 'Record : equality> (records: 'Record[]) =
    use stream = new MemoryStream()
    let parquetWriter = ParquetStreamWriter<'Record>(stream)
    parquetWriter.WriteHeader()
    parquetWriter.WriteRowGroup(records)
    parquetWriter.WriteFooter()
    stream.Seek(0, SeekOrigin.Begin) |> ignore
    let parquetReader = ParquetStreamReader<'Record>(stream)
    parquetReader.ReadMetaData()
    let roundtrippedRecords = parquetReader.ReadRowGroup(0)
    test <@ roundtrippedRecords = records @>

[<Property>]
let ``record with bool field`` records =
    testRecordRoundtrip<{| Field: bool |}> records
