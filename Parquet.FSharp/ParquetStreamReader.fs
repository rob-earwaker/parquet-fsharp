namespace Parquet.FSharp

open System
open System.IO

type ParquetStreamReader<'Record>(stream: Stream) =
    let magicNumber = "PAR1"
    let recordInfo = RecordInfo.ofRecord typeof<'Record>

    member this.ReadMetaData() =
        Stream.seekBackwardsFromEnd stream 8
        let fileMetaDataSize = Stream.readInt32 stream
        let magicNumber' = Stream.readAscii stream 4
        if magicNumber' <> magicNumber then
            failwith $"expected magic number '{magicNumber}' at end of file"
        Stream.seekBackwardsFromEnd stream (8 + fileMetaDataSize)
        let fileMetaDataBytes = Stream.readBytes stream fileMetaDataSize
        let fileMetaData =
            Thrift.Serialization.deserialize<Thrift.FileMetaData> fileMetaDataBytes
        ()
