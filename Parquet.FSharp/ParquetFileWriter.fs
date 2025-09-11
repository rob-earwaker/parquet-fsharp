namespace Parquet.FSharp

open FSharp.Reflection
open System
open System.IO
open System.Text
open System.Threading
open Thrift
open Thrift.Protocol
open Thrift.Transport.Client

type ParquetStreamWriter<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordTypeInfo = RecordTypeInfo.ofRecord<'Record>
    let fileMetaData =
        FileMetaData(
            Version = 1,
            Schema = Schema.toThriftSchema recordTypeInfo,
            Num_rows = 0,
            Row_groups = ResizeArray(),
            Created_by =
                $"{Library.Name.Value} version {Library.Version.Value}"
                + $" (build {Library.Revision.Value})")

    member this.WriteHeader() =
        Stream.writeAscii stream magicNumber

    member this.WriteRowGroup(records: 'Record seq) =
        let records = Array.ofSeq records
        let rowGroup =
            RowGroup(
                Columns = ResizeArray(),
                Total_byte_size = 0,
                Num_rows = records.Length,
                File_offset = stream.Position)
        let columns = Dremel.shred records
        for column in columns do
            let columnChunk =
                ColumnChunk(
                    File_offset = 0,
                    Meta_data = ColumnMetaData(
                        Type = Type.BOOLEAN))
            //Update row group column list and total size
            ()

    member this.WriteFooter() =
        use transport = new TMemoryBufferTransport(TConfiguration())
        use protocol = new TCompactProtocol(transport)
        fileMetaData.WriteAsync(protocol, CancellationToken.None)
        |> Async.AwaitTask
        |> Async.RunSynchronously
        let metaDataBytes = transport.GetBuffer()
        Stream.writeBytes stream metaDataBytes
        Stream.writeInt32 stream metaDataBytes.Length
        Stream.writeAscii stream magicNumber

    member this.Dispose() =
        this.WriteFooter()

    interface IDisposable with
        member this.Dispose() = this.Dispose()
