module Parquet.FSharp.Encoding

open System
open System.Collections
open System.IO

module Bool =
    module Plain =
        let encode (values: bool[]) =
            let bitArray = BitArray(values)
            let byteCount =
                if bitArray.Length % 8 = 0
                then bitArray.Length / 8
                else bitArray.Length / 8 + 1
            let bytes = Array.zeroCreate<byte> byteCount
            bitArray.CopyTo(bytes, 0)
            bytes

        let decode stream count =
            let byteCount =
                if count % 8 = 0
                then count / 8
                else count / 8 + 1
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
        let private writeRunLengthRun stream count value byteWidth =
            let header = uint32 count <<< 1
            Stream.writeUleb128 stream header
            Stream.writeInt32FixedWidth stream value byteWidth

        let private runLengthEncode bitWidth (values: int[]) =
            // Round up bit width to nearest byte to get byte width of values.
            let byteWidth =
                if bitWidth % 8 = 0
                then bitWidth / 8
                else bitWidth / 8 + 1
            use stream = new MemoryStream()
            let mutable previousValue = values[0]
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

        let encode values (maxValue: int) =
            // Calculate fixed bit width based on max value. The maximum value
            // {i} that can be stored in {n} bits is {2^n - 1}.
            //   >> i <= 2^n - 1
            //   >> i + 1 <= 2^n
            //   >> log2(i + 1) <= n
            //   >> ceil(log2(i + 1)) = n
            // Therefore, the minimum bit width {n} required to store value {i}
            // is {ceil(log2(i + 1))}.
            let bitWidth = int (Math.Ceiling(Math.Log(float maxValue + 1., 2)))
            let data = runLengthEncode bitWidth values
            use stream = new MemoryStream()
            Stream.writeInt32 stream data.Length
            Stream.writeBytes stream data
            stream.ToArray()

module Int64 =
    module Plain =
        let encode (values: int64[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt64 stream value
            stream.ToArray()

module Float64 =
    module Plain =
        let encode (values: float[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeFloat64 stream value
            stream.ToArray()

module ByteArray =
    module Plain =
        let encode (values: byte[][]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt32 stream value.Length
                Stream.writeBytes stream value
            stream.ToArray()
