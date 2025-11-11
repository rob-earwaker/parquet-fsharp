module Parquet.FSharp.Encoding

open System
open System.Collections
open System.IO

/// For a given bit count, calculate the corresponding byte count by rounding up
/// to the nearest byte.
let private getByteCount bitCount =
    if bitCount % 8 = 0
    then bitCount / 8
    else bitCount / 8 + 1

module Bool =
    module Plain =
        let encode (values: bool[]) =
            let bitArray = BitArray(values)
            let byteCount = getByteCount bitArray.Length
            let bytes = Array.zeroCreate<byte> byteCount
            bitArray.CopyTo(bytes, 0)
            bytes

        let decode stream count =
            let byteCount = getByteCount count
            let bytes = Stream.readBytes stream byteCount
            let bitArray = BitArray(bytes)
            let values = Array.zeroCreate<bool> bitArray.Count
            bitArray.CopyTo(values, 0)
            Array.truncate count values

module Int32 =
    module Plain =
        let encode (values: int[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt32 stream value
            stream.ToArray()

        let decode stream count =
            Array.init count (fun _ -> Stream.readInt32 stream)

    module RunLengthBitPackingHybrid =
        let private getBitWidth (maxValue: int) =
            // Calculate fixed bit width based on max value. The maximum value
            // {i} that can be stored in {n} bits is {2^n - 1}.
            //   >> i <= 2^n - 1
            //   >> i + 1 <= 2^n
            //   >> log2(i + 1) <= n
            //   >> ceil(log2(i + 1)) = n
            // Therefore, the minimum bit width {n} required to store value {i}
            // is {ceil(log2(i + 1))}.
            int (Math.Ceiling(Math.Log(float maxValue + 1., 2)))

        let private writeRunLengthRun stream count value byteWidth =
            let header = uint32 count <<< 1
            Stream.writeUleb128 stream header
            Stream.writeInt32FixedWidth stream value byteWidth

        let private runLengthEncode (values: int[]) maxValue =
            let bitWidth = getBitWidth maxValue
            let byteWidth = getByteCount bitWidth
            use stream = new MemoryStream()
            // Initialise the previous value. If there are values in the array
            // then initialize to the first value. This prevents us detecting
            // the first value as the end of a run and therefore prevents us
            // writing an initial empty run. If there are no values in the array
            // then initialize to zero so that after the loop terminates we will
            // write an empty run with this default value. In practice it
            // shouldn't matter what this default value is chosen to be as it
            // will never be used when decoding.
            let mutable previousValue = if values.Length > 0 then values[0] else 0
            let mutable count = 0
            for value in values do
                if value = previousValue then
                    // Value hasn't changed. Update the count to say we've seen
                    // another of the same value.
                    count <- count + 1
                else
                    // Value has changed. Write the previous value and count as
                    // a new run. Update the previous value to the new value and
                    // set the count to say we've seen one of this value so far.
                    writeRunLengthRun stream count previousValue byteWidth
                    previousValue <- value
                    count <- 1
            // The loop will always terminate without writing the last run of
            // values, so write this last run.
            writeRunLengthRun stream count previousValue byteWidth
            stream.ToArray()

        let encodeWithLength values maxValue =
            let data = runLengthEncode values maxValue
            use stream = new MemoryStream()
            Stream.writeInt32 stream data.Length
            Stream.writeBytes stream data
            stream.ToArray()

        let decode stream count maxValue =
            let bitWidth = getBitWidth maxValue
            let byteWidth = getByteCount bitWidth
            let values = ResizeArray()
            while values.Count < count do
                // Read the header value for the next run.
                let header = Stream.readUleb128 stream
                // The least significant bit indicates whether the run uses run
                // length encoding or bit packing.
                let isBitPacked = (header &&& 1u) = 1u
                // The rest of the header value contains the number of values in
                // the run. Extract this by right-shifting by one to remove the
                // least significant bit.
                let valueCount = int (header >>> 1)
                if isBitPacked
                then failwith "unsupported"
                else
                    let value = Stream.readInt32FixedWidth stream byteWidth
                    values.AddRange(Seq.replicate valueCount value)
            if values.Count > count then
                failwith "count exceeded!"
            Array.ofSeq values

        let decodeWithLength stream count maxValue =
            // The data length is not currently used. We use the value count to
            // determine when we've finished decoding.
            let dataLength = Stream.readInt32 stream
            decode stream count maxValue

module Int64 =
    module Plain =
        let encode (values: int64[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt64 stream value
            stream.ToArray()

        let decode stream count =
            Array.init count (fun _ -> Stream.readInt64 stream)

module Float32 =
    module Plain =
        let encode (values: float32[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeFloat32 stream value
            stream.ToArray()

        let decode stream count =
            Array.init count (fun _ -> Stream.readFloat32 stream)

module Float64 =
    module Plain =
        let encode (values: float[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeFloat64 stream value
            stream.ToArray()

        let decode stream count =
            Array.init count (fun _ -> Stream.readFloat64 stream)

module ByteArray =
    module Plain =
        let encode (values: byte[][]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt32 stream value.Length
                Stream.writeBytes stream value
            stream.ToArray()

        let decode stream count =
            Array.init count (fun _ ->
                let size = Stream.readInt32 stream
                Stream.readBytes stream size)

module FixedLengthByteArray =
    module Plain =
        let encode (values: byte[][]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeBytes stream value
            stream.ToArray()

        let decode stream count size =
            Array.init count (fun _ ->
                Stream.readBytes stream size)
