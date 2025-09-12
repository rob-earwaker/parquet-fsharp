module Parquet.FSharp.Encoding

open System.Collections
open System.IO

module Plain =
    module Bool =
        let encode (values: bool[]) =
            let bitArray = BitArray(values)
            let byteCount =
                if bitArray.Length % 8 = 0
                then bitArray.Length / 8
                else bitArray.Length / 8 + 1
            let bytes = Array.zeroCreate<byte> byteCount
            bitArray.CopyTo(bytes, 0)
            bytes

    module Int32 =
        let encode (values: int[]) =
            use stream = new MemoryStream()
            for value in values do
                Stream.writeInt32 stream value
            stream.ToArray()
