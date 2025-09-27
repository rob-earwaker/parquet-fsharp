namespace Parquet.FSharp

open System
open System.IO

type private ColumnSchemaInfo = {
    SchemaElement: Thrift.SchemaElement
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int }

type ParquetStreamReader<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let mutable fileMetaData = null
    let mutable columnSchemaInfo = null

    member this.ReadMetaData() =
        Stream.seekBackwardsFromEnd stream 8
        let fileMetaDataSize = Stream.readInt32 stream
        let magicNumber' = Stream.readAscii stream 4
        if magicNumber' <> magicNumber then
            failwith $"expected magic number '{magicNumber}' at end of file"
        let fileMetaDataOffset = 8 + fileMetaDataSize
        Stream.seekBackwardsFromEnd stream fileMetaDataOffset
        fileMetaData <- Stream.readThrift<Thrift.FileMetaData> stream fileMetaDataSize
        columnSchemaInfo <-
            let columnSchemaInfo = ResizeArray()
            let mutable nextIndex = 1
            let rec processGroup numChildren maxRepetitionLevel maxDefinitionLevel =
                for _ in [ 0 .. numChildren - 1 ] do
                    let schemaElement = fileMetaData.Schema[nextIndex]
                    nextIndex <- nextIndex + 1
                    let maxRepetitionLevel =
                        if schemaElement.Repetition_type = Thrift.FieldRepetitionType.REPEATED
                        then maxRepetitionLevel + 1
                        else maxRepetitionLevel
                    let maxDefinitionLevel =
                        if schemaElement.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL
                            || schemaElement.Repetition_type = Thrift.FieldRepetitionType.REPEATED
                        then maxDefinitionLevel + 1
                        else maxDefinitionLevel
                    if schemaElement.__isset.num_children then
                        processGroup schemaElement.Num_children maxRepetitionLevel maxDefinitionLevel
                    else
                        columnSchemaInfo.Add({
                            ColumnSchemaInfo.SchemaElement = schemaElement
                            ColumnSchemaInfo.MaxRepetitionLevel = maxRepetitionLevel
                            ColumnSchemaInfo.MaxDefinitionLevel = maxDefinitionLevel })
            let rootElement = fileMetaData.Schema[0]
            let maxRepetitionLevel = 0
            let maxDefinitionLevel = 0
            processGroup rootElement.Num_children maxRepetitionLevel maxDefinitionLevel
            Array.ofSeq columnSchemaInfo

    member this.ReadRowGroup(index) =
        let rowGroup = fileMetaData.Row_groups[index]
        Stream.seekForwardsFromStart stream rowGroup.File_offset
        let columns =
            Seq.zip columnSchemaInfo rowGroup.Columns
            |> Seq.map this.ReadColumn
            |> Array.ofSeq
        Dremel.assemble<'Record> columns

    member private this.ReadColumn(schemaInfo: ColumnSchemaInfo, column: Thrift.ColumnChunk) =
        let metaData = column.Meta_data
        let totalCompressedSize = int metaData.Total_compressed_size
        let pageHeader = Stream.readThrift<Thrift.PageHeader> stream totalCompressedSize
        match pageHeader.Type with
        | Thrift.PageType.DATA_PAGE ->
            let dataPageHeader = pageHeader.Data_page_header
            let valueCount = dataPageHeader.Num_values
            let repetitionLevels =
                if schemaInfo.MaxRepetitionLevel = 0
                then Option.None
                else
                    match dataPageHeader.Repetition_level_encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Int32.Plain.decode stream valueCount
                        |> Option.Some
                    | _ -> failwith "unsupported"
            let definitionLevels =
                if schemaInfo.MaxDefinitionLevel = 0
                then Option.None
                else
                    match dataPageHeader.Definition_level_encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Int32.Plain.decode stream valueCount
                        |> Option.Some
                    | _ -> failwith "unsupported"
            // Determine the number of values we should be decoding. NULL values
            // are not encoded, so we don't count any values where the
            // definition level is less than the maximum as this implies a NULL
            // value.
            let encodedValueCount =
                match definitionLevels with
                | Option.None -> valueCount
                | Option.Some definitionLevels ->
                    definitionLevels
                    |> Array.filter (fun level -> level = schemaInfo.MaxDefinitionLevel)
                    |> Array.length
            let values =
                match metaData.Type with
                | Thrift.Type.BOOLEAN ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Bool.Plain.decode stream encodedValueCount
                        |> ColumnValues.Bool
                    | _ -> failwith "unsupported"
                | Thrift.Type.INT32 ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Int32.Plain.decode stream encodedValueCount
                        |> ColumnValues.Int32
                    | _ -> failwith "unsupported"
                | Thrift.Type.INT64 ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Int64.Plain.decode stream encodedValueCount
                        |> ColumnValues.Int64
                    | _ -> failwith "unsupported"
                | Thrift.Type.DOUBLE ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Float64.Plain.decode stream encodedValueCount
                        |> ColumnValues.Float64
                    | _ -> failwith "unsupported"
                | Thrift.Type.BYTE_ARRAY ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.ByteArray.Plain.decode stream encodedValueCount
                        |> ColumnValues.ByteArray
                    | _ -> failwith "unsupported"
                | _ -> failwith "unsupported"
            { Column.ValueCount = valueCount
              Column.Values = values
              Column.MaxRepetitionLevel = schemaInfo.MaxRepetitionLevel
              Column.MaxDefinitionLevel = schemaInfo.MaxDefinitionLevel
              Column.RepetitionLevels = repetitionLevels
              Column.DefinitionLevels = definitionLevels }
        | _ -> failwith $"unsupported page type '{pageHeader.Type}'"
