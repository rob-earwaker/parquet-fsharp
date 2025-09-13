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
        let columns = Dremel.shred records
        for column in columns do
            let repetitionLevelEncoding, repetitionLevelBytes =
                match column.RepetitionLevels with
                | Option.Some repetitionLevels ->
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Int32.encode repetitionLevels
                    encoding, bytes
                | Option.None -> Thrift.Encoding.PLAIN, [||]
            let definitionLevelEncoding, definitionLevelBytes =
                match column.DefinitionLevels with
                | Option.Some definitionLevels ->
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Int32.encode definitionLevels
                    encoding, bytes
                | Option.None -> Thrift.Encoding.PLAIN, [||]
            let valueType, valueEncoding, valueBytes =
                match column.Values.GetType().GetElementType() with
                | dotnetType when dotnetType = typeof<bool> ->
                    let values = column.Values :?> bool[]
                    let type' = Thrift.Type.BOOLEAN
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Bool.encode values
                    type', encoding, bytes
                | dotnetType when dotnetType = typeof<int> ->
                    let values = column.Values :?> int[]
                    let type' = Thrift.Type.INT32
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Plain.Int32.encode values
                    type', encoding, bytes
                | dotnetType -> failwith $"unsupported type {dotnetType.FullName}"
            let dataPageUncompressedSize =
                repetitionLevelBytes.Length
                + definitionLevelBytes.Length
                + valueBytes.Length
            let pageHeader =
                Thrift.PageHeader(
                    Type = Thrift.PageType.DATA_PAGE,
                    Uncompressed_page_size = dataPageUncompressedSize,
                    Compressed_page_size = dataPageUncompressedSize,
                    Data_page_header = Thrift.DataPageHeader(
                        Num_values = column.ValueCount,
                        Encoding = valueEncoding,
                        Definition_level_encoding = definitionLevelEncoding,
                        Repetition_level_encoding = repetitionLevelEncoding))
            let pageHeaderBytes = Thrift.Serialization.serialize pageHeader
            let dataPageOffset = stream.Position
            Stream.writeBytes stream pageHeaderBytes
            Stream.writeBytes stream repetitionLevelBytes
            Stream.writeBytes stream definitionLevelBytes
            Stream.writeBytes stream valueBytes
            let columnTotalUncompressedSize = stream.Position - dataPageOffset
            let columnChunk =
                Thrift.ColumnChunk(
                    File_offset = 0,
                    Meta_data = Thrift.ColumnMetaData(
                        Type = valueType,
                        Encodings = ResizeArray([ Thrift.Encoding.PLAIN ]),
                        Path_in_schema = ResizeArray([ column.FieldInfo.Name ]),
                        Codec = Thrift.CompressionCodec.UNCOMPRESSED,
                        Num_values = column.ValueCount,
                        Total_uncompressed_size = columnTotalUncompressedSize,
                        Total_compressed_size  = columnTotalUncompressedSize,
                        Data_page_offset = dataPageOffset))
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
