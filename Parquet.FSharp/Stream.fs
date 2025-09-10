module Parquet.FSharp.Stream

open System
open System.IO
open System.Text

let writeBytes (stream: Stream) (bytes: byte[]) =
    stream.Write(bytes, 0, bytes.Length)

let writeAscii stream (ascii: string) =
    let bytes = Encoding.ASCII.GetBytes(ascii)
    writeBytes stream bytes

let writeInt32 stream (int: int) =
    let bytes = BitConverter.GetBytes(int)
    writeBytes stream bytes
