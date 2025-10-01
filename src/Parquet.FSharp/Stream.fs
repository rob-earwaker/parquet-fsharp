module Parquet.FSharp.Stream

open System
open System.IO
open System.Text
open Thrift.Protocol

let seekBackwardsFromEnd (stream: Stream) count =
    stream.Seek(-count, SeekOrigin.End) |> ignore

let seekForwardsFromStart (stream: Stream) count =
    stream.Seek(count, SeekOrigin.Begin) |> ignore

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

let writeFloat32 stream (value: float32) =
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

let readByte (stream: Stream) =
    byte (stream.ReadByte())

let readBytes (stream: Stream) count =
    let bytes = Array.zeroCreate<byte> count
    stream.Read(bytes, 0, bytes.Length) |> ignore
    bytes

let readAscii stream count =
    let bytes = readBytes stream count
    Encoding.ASCII.GetString(bytes)

let readInt32 stream =
    let bytes = readBytes stream 4
    BitConverter.ToInt32(bytes, 0)

let readInt32FixedWidth stream byteWidth =
    let bytes = readBytes stream byteWidth
    let fullWidthBytes = Array.zeroCreate<byte> 4
    bytes.CopyTo(fullWidthBytes, 0)
    BitConverter.ToInt32(fullWidthBytes, 0)

let readInt64 stream =
    let bytes = readBytes stream 8
    BitConverter.ToInt64(bytes, 0)

let readFloat32 stream =
    let bytes = readBytes stream 4
    BitConverter.ToSingle(bytes, 0)

let readFloat64 stream =
    let bytes = readBytes stream 8
    BitConverter.ToDouble(bytes, 0)

let readUleb128 stream =
    let rec readUleb128' stream shift =
        let nextByte = readByte stream
        // The least significant 7 bits of the byte contain the next part of the
        // value. Read this by performing a bitwise AND with {0b01111111} and
        // shift by the current shift level.
        let value = uint32 (nextByte &&& 0b01111111uy) <<< shift
        // The most significant bit of the byte indicates whether there are more
        // bytes to come. If this bit is zero then we've reached the end of our
        // value. If it's one then there are more bytes to come. Read the bit by
        // performing a bitwise AND with {0b10000000}.
        if (nextByte &&& 0b10000000uy) = 0b00000000uy
        // There are no more bytes to come, so just return the current value.
        then value
        // There are more bytes to come. Increment the shift by seven and read
        // the remainder of the value. We then perform a bitwise OR with the
        // current value to combine them. The shift ensures these values don't
        // overlap.
        else readUleb128' stream (shift + 7) ||| value
    readUleb128' stream 0

let readThrift<'Value when 'Value :> TBase and 'Value : (new : unit -> 'Value)> (stream: Stream) maxSize =
    // Jump through some weird hoops because there's no option in the thrift
    // library to leave the stream open when reading from it. In some cases we
    // don't know the size of the value, so we read more than we need and then
    // figure out how many bytes were read so we can reset the stream position.
    // Not sure there's a better way of doing this unless the thrift library is
    // updated or the protocol is implemented in this library!
    let startPosition = stream.Position
    use temporaryStream = new MemoryStream(readBytes stream maxSize)
    let value, bytesRead = Thrift.Serialization.deserializeFrom<'Value> temporaryStream
    let endPosition = startPosition + bytesRead
    seekForwardsFromStart stream endPosition
    value
