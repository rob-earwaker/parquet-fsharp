namespace Parquet.FSharp

open Parquet.FSharp.Schema
open System
open System.IO

type ParquetStreamWriter<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordInfo = RecordInfo.ofRecord<'Record>
    let schema = recordInfo.Fields |> Array.map _.Schema |> Schema.create
    let fileMetaData =
        Thrift.FileMetaData(
            Version = 1,
            Schema = Schema.toThrift schema,
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
            Thrift.RowGroup(
                Columns = ResizeArray(),
                Total_byte_size = 0,
                Num_rows = records.Length,
                File_offset = stream.Position)
        for column in Dremel.shred records do
            let columnMetaData =
                Thrift.ColumnMetaData(
                    Encodings = ResizeArray([ Thrift.Encoding.PLAIN ]),
                    Path_in_schema = ResizeArray([ column.FieldInfo.Name ]),
                    Codec = Thrift.CompressionCodec.UNCOMPRESSED,
                    Num_values = column.Values.Length,
                    // Assume only data pages for now.
                    Data_page_offset = stream.Position)
            let dataPageHeader =
                Thrift.DataPageHeader(
                    Num_values = column.Values.Length,
                    Definition_level_encoding = Thrift.Encoding.RLE,
                    Repetition_level_encoding = Thrift.Encoding.RLE)
            let type', encoding, dataBytes =
                match column.Values.GetType().GetElementType() with
                | DotnetType.Bool ->
                    let values = column.Values :?> bool[]
                    let type' = Thrift.Type.BOOLEAN
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Bool.encode values
                    type', encoding, bytes
                | DotnetType.Int32 ->
                    let values = column.Values :?> int[]
                    let type' = Thrift.Type.INT32
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Int32.encode values
                    type', encoding, bytes
                | dotnetType -> failwith $"unsupported type {dotnetType.FullName}"
            dataPageHeader.Encoding <- encoding
            let pageHeader =
                Thrift.PageHeader(
                    Type = Thrift.PageType.DATA_PAGE,
                    Uncompressed_page_size = dataBytes.Length,
                    Compressed_page_size = dataBytes.Length,
                    Data_page_header = dataPageHeader)
            let pageHeaderBytes = Thrift.Serialization.serialize pageHeader
            let startPosition = stream.Position
            Stream.writeBytes stream pageHeaderBytes
            Stream.writeBytes stream dataBytes
            columnMetaData.Type <- type'
            columnMetaData.Total_uncompressed_size <- stream.Position - startPosition
            columnMetaData.Total_compressed_size <- columnMetaData.Total_uncompressed_size
            let columnChunk =
                Thrift.ColumnChunk(
                    File_offset = 0,
                    Meta_data = columnMetaData)
            rowGroup.Columns.Add(columnChunk)
        fileMetaData.Row_groups.Add(rowGroup)
        fileMetaData.Num_rows <- fileMetaData.Num_rows + rowGroup.Num_rows

    member this.WriteFooter() =
        let metaDataBytes = Thrift.Serialization.serialize fileMetaData
        Stream.writeBytes stream metaDataBytes
        Stream.writeInt32 stream metaDataBytes.Length
        Stream.writeAscii stream magicNumber

    member this.Dispose() =
        this.WriteFooter()

    interface IDisposable with
        member this.Dispose() = this.Dispose()
