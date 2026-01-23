# Parquet.FSharp

[![NuGet Version](https://img.shields.io/nuget/v/Parquet.FSharp?style=flat-square&label=NuGet&logo=nuget)](https://www.nuget.org/packages/Parquet.FSharp)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/rob-earwaker/parquet-fsharp/build.yml?style=flat-square&label=Build&logo=github)](https://github.com/rob-earwaker/parquet-fsharp/actions/workflows/build.yml?query=branch%3Amain)

An F# serailization library for the [Apache Parquet](https://parquet.apache.org/) file format, built on top of the fantastic [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) library. **Parquet.FSharp** adds first-class support for F# types such as records, options, lists and discriminated unions, whilst maintaining the performance of **Parquet.Net**.

- [Quickstart](#quickstart)
- [Supported Types](#supported-types)
  - [Booleans](#booleans)
  - [Numeric Types](#numeric-types)
  - [GUIDs](#guids)
  - [Date Times](#date-times)
  - [Strings](#strings)
  - [Byte Arrays](#byte-arrays)
  - [Arrays, Lists \& Enumerables](#arrays-lists--enumerables)
  - [Records, Structs \& Classes](#records-structs--classes)
  - [Optional Types](#optional-types)
  - [Discriminated Unions](#discriminated-unions)
- [Roadmap](#roadmap)
  - [Extend Supported Types](#extend-supported-types)
  - [Serialization Options](#serialization-options)
  - [Serialization Attributes](#serialization-attributes)
  - [Custom Serializers](#custom-serializers)
  - [Improved Error Handling](#improved-error-handling)

## Quickstart

```fsharp
open Parquet.FSharp
open System.IO

type Shape =
    | Circle of radius:int
    | Square of sideLength:int
    | Rectangle of height:int * width:int

type Node = {
    Id: int
    Shape: Shape
    Scale: float option
    Children: int list }

let nodes = [|
    { Id = 0; Shape = Square 1        ; Scale = None    ; Children = [ 1; 2 ] }
    { Id = 1; Shape = Circle 2        ; Scale = Some 1.5; Children = [ 4 ]    }
    { Id = 2; Shape = Square 3        ; Scale = Some 0.5; Children = [ 3 ]    }
    { Id = 3; Shape = Rectangle (1, 2); Scale = None    ; Children = [ 4 ]    }
    { Id = 4; Shape = Circle 1        ; Scale = Some 2.0; Children = []       } |]

// Serialize to file
use file = File.OpenWrite("./nodes.parquet")
ParquetSerializer.Serialize(nodes, file)

// Deserialize from file
use file = File.OpenRead("./nodes.parquet")
let nodes = ParquetSerializer.Deserialize<Node>(file)
```

<sub>[[Return to top]](#parquetfsharp)</sub>

## Supported Types

<sub>[[Return to top]](#parquetfsharp)</sub>

### Booleans

Applies to: `bool`

Booleans are serialized as required values by default. They can be deserialized from either required or optional boolean values. When deserialized from optional values, any null values encountered will result in a `SerializationException`.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Numeric Types

Applies to: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float[64]`, `decimal`

Numeric types are serialized as required values by default. They can be deserialized from either required or optional values. When deserialized from optional values, any null values encountered will result in a `SerializationException`.

For deserialization, the target .NET numeric type does not have to match the source Parquet numeric type. Numeric type compatibility is determined based on whether the source type is implicitly convertible to the target type, e.g. a field of type `int32` can be deserialized from a field of type `int16`. The following compatibility table lists the possible combinations - largely derived from [.NET Implicit Numerical Conversions](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/numeric-conversions#implicit-numeric-conversions):

| Target Type | Supported Source Types |
|-|-|
| `int8` | `int8` |
| `int16` | `int16`, `int8`, `uint8` |
| `int32` | `int32`, `int16`, `int8`, `uint16`, `uint8` |
| `int64` | `int64`, `int32`, `int16`, `int8`, `uint32`, `uint16`, `uint8` |
| `uint8` | `uint8` |
| `uint16` | `uint16`, `uint8` |
| `uint32` | `uint32`, `uint16`, `uint8` |
| `uint64` | `uint64`, `uint32`, `uint16`, `uint8` |
| `float32` | `float32`, `int16`, `int8`, `uint16`, `uint8` |
| `float[64]` | `float[64]`, `float32`, `int32`, `int16`, `int8`, `uint32`, `uint16`, `uint8` |
| `decimal` | `decimal`, `int64`, `int32`, `int16`, `int8`, `uint64`, `uint32`, `uint16`, `uint8` |

<sub>[[Return to top]](#parquetfsharp)</sub>

### GUIDs

Applies to: `Guid`

GUIDs are serialized as required values by default. They can be deserialized from either required or optional GUID values. When deserialized from optional values, any null values encountered will result in a `SerializationException`.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Date Times

Applies to: `DateTime`, `DateTimeOffset`

Date times are serialized as required UTC values with microsecond precision by default. They can be deserialized from either required or optional date time values. When deserialized from optional values, any null values encountered will result in a `SerializationException`.

Since `DateTime` values have an associated `DateTimeKind`, which is one of `Unspecified`, `Utc` or `Local`, conversion to UTC can be ambiguous. Default serialization does not make any assumptions or do any implicit conversions, so any `DateTime` values that are not defined with `DateTimeKind.Utc` will result in a `SerializationException`.

`DateTimeOffset` values always map to a specific instant in time, so can always be converted to UTC in an unambiguous way. During serialization, `DateTimeOffset` values will be converted to their UTC equivalent. This means that the offset information is lost, but the serialized value is guaranteed to identify the same instant in time.

Both `DateTime` and `DateTimeOffset` use 'ticks' as their base unit, where each tick represents a 100 nanosecond period. Since the default precision is microseconds, serialization results in a slight truncation, equivalent to rounding the values down to the nearest 10 ticks.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Strings

Applies to: `string`

Strings are serialized as required values by default, despite being reference types and having null as a valid value. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating strings as required provides a guarantee that any serialized values are not null. If a null value is encountered during serialization, a `SerializationException` will be raised.

Strings can be deserialized from optional values as well as required values, but the same null guarantee is provided, so any null values encountered during deserialization will still result in an exception.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Byte Arrays

Applies to: `byte[]`

Byte arrays are not treated the same as other array types since Parquet has native support for them. This means that instead of being treated as repeated values they are treated as atomic values.

Byte arrays are serialized as required values by default, despite being reference types and having null as a valid value. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating byte arrays as required provides a guarantee that any serialized values are not null. If a null value is encountered during serialization, a `SerializationException` will be raised.

Byte arrays can be deserialized from optional values as well as required values, but the same null guarantee is provided, so any null values encountered during deserialization will still result in an exception.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Arrays, Lists & Enumerables

Applies to: `'Element[]`, `'Element list`, `ResizeArray<'Element>`

Sequences of values are stored as [Parquet lists](https://github.com/apache/parquet-format/blob/4b1c72c837bec5b792b2514f0057533030fcedf8/LogicalTypes.md#lists), which contain repeated elements, analagous to the `'Element seq` or  `IEnumerable<'Element>` .NET types.

Supported sequence types are serialized as required Parquet lists by default, even for sequences that allow null as a valid value. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating sequences as required provides a guarantee that any serialized sequences are not null. If a null sequence is encountered during serialization, a `SerializationException` will be raised.

Sequences can be deserialized from optional Parquet lists as well as required lists, but the same null guarantee is provided, so any null lists encountered during deserialization will still result in an exception.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Records, Structs & Classes

Applies to: `'FSharpRecord`

TODO: Add docs

<sub>[[Return to top]](#parquetfsharp)</sub>

### Optional Types

Applies to: `'Value option`, `Nullable<'Value>`

TODO: Add docs

<sub>[[Return to top]](#parquetfsharp)</sub>

### Discriminated Unions

Applies to: `'FSharpUnion`

TODO: Add docs

<sub>[[Return to top]](#parquetfsharp)</sub>

## Roadmap

The following features and improvements are on the roadmap, in no particular order.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Extend Supported Types

The following types are not currently supported but will likely be added in the future:
- `'Enum`
- `DateOnly`
- `TimeOnly`
- `TimeSpan`
- `Interval` (from **Parquet.Net**)
- `BigDecimal` (from **Parquet.Net**)
- `BigInteger` (from **System.Numerics**)

<sub>[[Return to top]](#parquetfsharp)</sub>

### Serialization Options

The ability to specify options when serializing and deserializing to allow finer-grained control of serialization behaviour. This will hopefully include the ability to apply options to either all fields that are being serialized or just to specific fields. Configuration options could include the ability to:

- Override field names and union case names.
- Serialize fields that would normally be serialized as required fields as optional instead.
- Allow serialization and deserialization of null values for reference types.
- Treat date time values as local rather than UTC.
- Allow millisecond and nanosecond precision for date times in addition to the default microsecond precision.
- Specify the precision and scale of decimal values.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Serialization Attributes

Attributes that can be applied to types and fields to allow the above serialization options to be defined as part of type definitions rather than requiring configuration at the point of serialization.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Custom Serializers

Allow the definition of custom serializers and deserializers that can be used to override the default serialization behaviour provided by the library for specific types, or allow serialization of types that aren't supported by the library. The default serialization behaviour defined in the library is already set up in this way, but the list of registered converters is not yet configurable and just contains a default converter for each supported type.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Improved Error Handling

For performance reasons, serialization is implemented by generating, compiling and executing [Expression Trees](https://learn.microsoft.com/en-us/dotnet/csharp/advanced-topics/expression-trees/), in a similar fashion to **Parquet.Net**. This means that errors are not always easy to trace back to the code that caused them. To improve on this, extra exception handling could be added into the generated expression trees to provide more information about the expression that was being executed when the exception occurred, e.g. to identify the specific converter function that was being called.

<sub>[[Return to top]](#parquetfsharp)</sub>
