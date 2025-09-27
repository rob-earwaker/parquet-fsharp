namespace Parquet.FSharp

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
                $"{Library.Name} version {Library.Version}"
                + $" (build {Library.Revision})")

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
                File_offset = stream.Position,
                Ordinal = int16 fileMetaData.Row_groups.Count)
        let columns = Dremel.shred records
        for columnPath, column in Array.zip columnPaths columns do
            let repetitionLevelEncoding, repetitionLevelBytes =
                match column.RepetitionLevels with
                | Option.Some repetitionLevels ->
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Int32.Plain.encode repetitionLevels
                    encoding, bytes
                | Option.None -> Thrift.Encoding.PLAIN, [||]
            let definitionLevelEncoding, definitionLevelBytes =
                match column.DefinitionLevels with
                | Option.Some definitionLevels ->
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Int32.Plain.encode definitionLevels
                    encoding, bytes
                | Option.None -> Thrift.Encoding.PLAIN, [||]
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
                | ColumnValues.Int64 values ->
                    let type' = Thrift.Type.INT64
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Int64.Plain.encode values
                    type', encoding, bytes
                | ColumnValues.Float64 values ->
                    let type' = Thrift.Type.DOUBLE
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.Float64.Plain.encode values
                    type', encoding, bytes
                | ColumnValues.ByteArray values ->
                    let type' = Thrift.Type.BYTE_ARRAY
                    let encoding = Thrift.Encoding.PLAIN
                    let bytes = Encoding.ByteArray.Plain.encode values
                    type', encoding, bytes
            let dataPageUncompressedSize =
                repetitionLevelBytes.Length
                + definitionLevelBytes.Length
                + valueBytes.Length
            let dataPageHeader =
                Thrift.PageHeader(
                    Type = Thrift.PageType.DATA_PAGE,
                    Uncompressed_page_size = dataPageUncompressedSize,
                    Compressed_page_size = dataPageUncompressedSize,
                    Data_page_header = Thrift.DataPageHeader(
                        Num_values = column.ValueCount,
                        Encoding = valueEncoding,
                        Definition_level_encoding = definitionLevelEncoding,
                        Repetition_level_encoding = repetitionLevelEncoding))
            let dataPageHeaderBytes = Thrift.Serialization.serialize dataPageHeader
            let dataPageOffset = stream.Position
            Stream.writeBytes stream dataPageHeaderBytes
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
            rowGroup.Total_byte_size <-
                rowGroup.Total_byte_size
                + columnChunk.Meta_data.Total_uncompressed_size
            rowGroup.Total_compressed_size <-
                rowGroup.Total_compressed_size
                + columnChunk.Meta_data.Total_compressed_size
        fileMetaData.Row_groups.Add(rowGroup)
        fileMetaData.Num_rows <- fileMetaData.Num_rows + rowGroup.Num_rows

    member this.WriteFooter() =
        let fileMetaDataBytes = Thrift.Serialization.serialize fileMetaData
        Stream.writeBytes stream fileMetaDataBytes
        Stream.writeInt32 stream fileMetaDataBytes.Length
        Stream.writeAscii stream magicNumber
