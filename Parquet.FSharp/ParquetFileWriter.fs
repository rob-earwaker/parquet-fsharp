namespace Parquet.FSharp

open Parquet.FSharp.Schema
open System
open System.IO

type ParquetStreamWriter<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let schema = Schema.ofRecordInfo recordInfo
    let fileMetaData =
        Thrift.FileMetaData(
            Version = 1,
            Schema = Schema.toThrift schema,
            Num_rows = 0,
            Row_groups = ResizeArray(),
            Created_by =
                $"{Library.Name.Value} version {Library.Version.Value}"
                + $" (build {Library.Revision.Value})")

    let columnPaths =
        let columnPaths = ResizeArray()
        let currentPath = ResizeArray()
        let mutable nextIndex = 1
        let rec processGroup numChildren =
            for _ in [ 0 .. numChildren - 1 ] do
                let schemaElement = fileMetaData.Schema[nextIndex]
                nextIndex <- nextIndex + 1
                currentPath.Add(schemaElement.Name)
                if schemaElement.__isset.num_children then
                    processGroup schemaElement.Num_children
                else
                    columnPaths.Add(Array.ofSeq currentPath)
                currentPath.RemoveAt(currentPath.Count - 1)
        let rootElement = fileMetaData.Schema[0]
        processGroup rootElement.Num_children
        Array.ofSeq columnPaths

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
        for columnPath, column in Array.zip columnPaths columns do
            let repetitionLevelEncoding, repetitionLevelBytes =
                match column.RepetitionLevels with
                | Option.Some repetitionLevels ->
                    let encoding = Thrift.Encoding.RLE
                    let bytes =
                        Encoding.Int32.RunLengthBitPackingHybrid.encode
                            repetitionLevels
                            column.MaxRepetitionLevel
                    encoding, bytes
                | Option.None -> Thrift.Encoding.RLE, [||]
            let definitionLevelEncoding, definitionLevelBytes =
                match column.DefinitionLevels with
                | Option.Some definitionLevels ->
                    let encoding = Thrift.Encoding.RLE
                    let bytes =
                        Encoding.Int32.RunLengthBitPackingHybrid.encode
                            definitionLevels
                            column.MaxDefinitionLevel
                    encoding, bytes
                | Option.None -> Thrift.Encoding.RLE, [||]
            let valueType, valueEncoding, valueBytes =
                match column.Values with
                | ColumnValues.Bool values ->
                    let type' = Thrift.Type.BOOLEAN
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Bool.Plain.encode values
                    type', encoding, bytes
                | ColumnValues.Int32 values ->
                    let type' = Thrift.Type.INT32
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Int32.Plain.encode values
                    type', encoding, bytes
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
            let columnEncodings =
                set [ repetitionLevelEncoding; definitionLevelEncoding; valueEncoding ]
                |> Seq.sort
                |> ResizeArray
            let columnTotalUncompressedSize = stream.Position - dataPageOffset
            let columnChunk =
                Thrift.ColumnChunk(
                    File_offset = 0,
                    Meta_data = Thrift.ColumnMetaData(
                        Type = valueType,
                        Encodings = columnEncodings,
                        Path_in_schema = ResizeArray(columnPath),
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
