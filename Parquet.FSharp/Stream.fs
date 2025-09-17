module Parquet.FSharp.Stream

open System
open System.IO
open System.Text

let writeByte (stream: Stream) byte =
    stream.WriteByte(byte)

let writeBytes (stream: Stream) (bytes: byte[]) =
    stream.Write(bytes, 0, bytes.Length)

let writeAscii stream (value: string) =
    let bytes = Encoding.ASCII.GetBytes(value)
    writeBytes stream bytes

let writeInt32 stream (value: int) =
    let bytes = BitConverter.GetBytes(value)
    writeBytes stream bytes

let writeInt32FixedWidth stream (value: int) byteWidth =
    let bytes = BitConverter.GetBytes(value)
    writeBytes stream bytes[0 .. byteWidth - 1]

let writeInt64 stream (value: int64) =
    let bytes = BitConverter.GetBytes(value)
    writeBytes stream bytes

let writeFloat64 stream (value: float) =
    let bytes = BitConverter.GetBytes(value)
    writeBytes stream bytes

let rec writeUleb128 stream (value: uint32) =
    // Create the next byte by extracting least significant 7 bits of the
    // value using a bitwise AND with {0b01111111}.
    let nextByte = byte (value &&& 0b01111111u)
    // Now that we've captured the least significant 7 bits, we chop them
    // off the value by left-shifting.
    let remainingValue = value >>> 7
    // If the remaining value is zero then there is no more information to
    // write so we indicate that this is the last byte of our integer by
    // leaving the most significant bit as zero. Otherwise, we indicate that
    // there are more bytes to come by setting the most significant bit.
    // This is done using a bitwise OR with {0b10000000}.
    if remainingValue = 0u
    then writeByte stream nextByte
    else
        let nextByte = nextByte ||| 0b10000000uy
        writeByte stream nextByte
        writeUleb128 stream remainingValue
