namespace Parquet.FSharp

open System
open System.IO

type ParquetStreamReader<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let mutable fileMetaData = null
    let mutable columnSchemaElements = null

    member this.ReadMetaData() =
        Stream.seekBackwardsFromEnd stream 8
        let fileMetaDataSize = Stream.readInt32 stream
        let magicNumber' = Stream.readAscii stream 4
        if magicNumber' <> magicNumber then
            failwith $"expected magic number '{magicNumber}' at end of file"
        let fileMetaDataOffset = 8 + fileMetaDataSize
        Stream.seekBackwardsFromEnd stream fileMetaDataOffset
        fileMetaData <- Stream.readThrift<Thrift.FileMetaData> stream fileMetaDataSize
        columnSchemaElements <-
            fileMetaData.Schema
            |> Array.ofSeq
            |> Array.filter (fun schemaElement ->
                schemaElement.Num_children = 0)

    member this.ReadRowGroup(index) =
        let rowGroup = fileMetaData.Row_groups[index]
        Stream.seekForwardsFromStart stream rowGroup.File_offset
        let columns =
            Seq.zip columnSchemaElements rowGroup.Columns
            |> Seq.map this.ReadColumn
            |> Array.ofSeq
        ()

    member private this.ReadColumn(schemaElement: Thrift.SchemaElement, column: Thrift.ColumnChunk) =
        let metaData = column.Meta_data
        let totalCompressedSize = int metaData.Total_compressed_size
        let pageHeader = Stream.readThrift<Thrift.PageHeader> stream totalCompressedSize
        match pageHeader.Type with
        | Thrift.PageType.DATA_PAGE ->
            let dataPageHeader = pageHeader.Data_page_header
            // TODO: Assume these for now, but should be derived from the schema.
            let maxRepetitionLevel = 0
            let maxDefinitionLevel = 0
            let repetitionLevels =
                if maxRepetitionLevel = 0
                then Option.None
                else failwith "unsupported"
            let definitionLevels =
                if maxDefinitionLevel = 0
                then Option.None
                else failwith "unsupported"
            let valueCount = dataPageHeader.Num_values
            // Determine the number of values we should be decoding. NULL values
            // are not encoded, so we don't count any values where the
            // definition level is less than the maximum as this implies a NULL
            // value.
            let encodedValueCount =
                match definitionLevels with
                | Option.None -> valueCount
                | Option.Some definitionLevels ->
                    definitionLevels
                    |> Array.filter (fun level -> level = maxDefinitionLevel)
                    |> Array.length
            let values =
                match metaData.Type with
                | Thrift.Type.INT32 ->
                    match dataPageHeader.Encoding with
                    | Thrift.Encoding.PLAIN ->
                        Encoding.Int32.Plain.decode stream encodedValueCount
                        |> ColumnValues.Int32
                    | _ -> failwith "unsupported"
                | _ -> failwith "unsupported"
            { Column.ValueCount = valueCount
              Column.Values = values
              Column.MaxRepetitionLevel = maxRepetitionLevel
              Column.MaxDefinitionLevel = maxDefinitionLevel
              Column.RepetitionLevels = repetitionLevels
              Column.DefinitionLevels = definitionLevels }
        | _ -> failwith $"unsupported page type '{pageHeader.Type}'"
