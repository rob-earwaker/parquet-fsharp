module internal Parquet.FSharp.DotnetType

open FSharp.Reflection
open System

// TODO: Check how much of this is actually used.

let private ActivePatternTypeMatch<'Type> dotnetType =
    if dotnetType = typeof<'Type>
    then Option.Some ()
    else Option.None

let (|Bool|_|) = ActivePatternTypeMatch<bool>
let (|Int8|_|) = ActivePatternTypeMatch<int8>
let (|Int16|_|) = ActivePatternTypeMatch<int16>
let (|Int32|_|) = ActivePatternTypeMatch<int>
let (|Int64|_|) = ActivePatternTypeMatch<int64>
let (|UInt8|_|) = ActivePatternTypeMatch<uint8>
let (|UInt16|_|) = ActivePatternTypeMatch<uint16>
let (|UInt32|_|) = ActivePatternTypeMatch<uint>
let (|UInt64|_|) = ActivePatternTypeMatch<uint64>
let (|Float32|_|) = ActivePatternTypeMatch<float32>
let (|Float64|_|) = ActivePatternTypeMatch<float>
let (|Decimal|_|) = ActivePatternTypeMatch<decimal>
let (|Guid|_|) = ActivePatternTypeMatch<Guid>
let (|DateTime|_|) = ActivePatternTypeMatch<DateTime>
let (|DateTimeOffset|_|) = ActivePatternTypeMatch<DateTimeOffset>
let (|String|_|) = ActivePatternTypeMatch<string>
let (|ByteArray|_|) = ActivePatternTypeMatch<byte[]>

let isGenericType<'GenericType> (dotnetType: Type) =
    dotnetType.IsGenericType
    && dotnetType.GetGenericTypeDefinition() = typedefof<'GenericType>
    
let private ActivePatternGenericTypeMatch<'GenericType> (dotnetType: Type) =
    if isGenericType<'GenericType> dotnetType
    then Option.Some ()
    else Option.None

let (|GenericList|_|) = ActivePatternGenericTypeMatch<ResizeArray<_>>
let (|FSharpList|_|) = ActivePatternGenericTypeMatch<list<_>>
let (|Nullable|_|) = ActivePatternGenericTypeMatch<Nullable<_>>
let (|Option|_|) = ActivePatternGenericTypeMatch<option<_>>

let (|Array1d|_|) (dotnetType: Type) =
    if dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1
    then Option.Some ()
    else Option.None

let (|Record|_|) dotnetType =
    if FSharpType.IsRecord(dotnetType)
    then Option.Some ()
    else Option.None

let (|Union|_|) dotnetType =
    if FSharpType.IsUnion(dotnetType)
    then Option.Some ()
    else Option.None
