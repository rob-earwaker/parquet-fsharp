# Parquet.FSharp

[![NuGet Version](https://img.shields.io/nuget/v/Parquet.FSharp?style=flat-square&label=NuGet&logo=nuget)](https://www.nuget.org/packages/Parquet.FSharp)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/rob-earwaker/parquet-fsharp/build.yml?style=flat-square&label=Build&logo=github)](https://github.com/rob-earwaker/parquet-fsharp/actions/workflows/build.yml?query=branch%3Amain)

An F# serialization library for the [Apache Parquet](https://parquet.apache.org/) file format, built on top of the fantastic [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) library. **Parquet.FSharp** adds first-class support for F# types such as records, options, lists and discriminated unions, whilst maintaining the performance of **Parquet.Net**.

>NOTE: **Parquet.FSharp** is in its initial development phase - the behaviour and public API may change between minor version increments. Feedback, ideas and feature requests are all welcome!

- [Quickstart](#quickstart)
- [Supported Types](#supported-types)
  - [Booleans](#booleans)
  - [Numeric Types](#numeric-types)
  - [GUIDs](#guids)
  - [Enums](#enums)
  - [Durations](#durations)
  - [Date Times](#date-times)
  - [Strings](#strings)
  - [Byte Arrays](#byte-arrays)
  - [Lists \& Arrays](#lists--arrays)
  - [Records](#records)
  - [Optional Types](#optional-types)
  - [Discriminated Unions](#discriminated-unions)
    - [Enumeration Unions](#enumeration-unions)
    - [Single-Case Unions](#single-case-unions)
    - [Multi-Case Unions](#multi-case-unions)
- [Customization](#customization)
  - [Field Settings](#field-settings)
  - [Value Settings](#value-settings)
- [Roadmap](#roadmap)
  - [Extend Supported Types](#extend-supported-types)
  - [Extend Attribute Support](#extend-attribute-support)
  - [Serialization Options](#serialization-options)
  - [Custom Converters](#custom-converters)
  - [Improved Error Handling](#improved-error-handling)
  - [Schema Evolution](#schema-evolution)

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

Boolean values map to the `BOOLEAN` primitive type in the Parquet file format. By default, they are serialized as required values and must be deserialized from required values.

The following settings can be used to customize serialization of boolean values via the `[<ParquetBool>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the value to be serialized as an optional value and allows it to be deserialized from an optional value. Since this type is non-nullable, if a null value is encountered during deserialization then a `SerializationException` will be raised. |

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

### Enums

Applies to: `'Enum` (with underlying type: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`)

Enums are serialized and deserialized as if they were their underlying integral numeric type - see [Numeric Types](#numeric-types) for details.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Durations

Applies to: `TimeSpan`

The Parquet format does not have built-in support for arbitrary durations. It only supports time-of-day durations via the [Time](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) logical type and positive durations with millisecond precision via the [Interval](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval) logical type. Neither of these are particularly compatible with the range of values that can be represented by a `TimeSpan`.

Due to the limitations above, `TimeSpan` values are serialized and deserialized as `int64` microsecond values. See [Numeric Types](#numeric-types) for details of `int64` serialization.

The `TimeSpan` type uses 'ticks' as a base unit, where each tick represents 100 nanoseconds. Since the default precision is microseconds, serialization results in a slight truncation, equivalent to rounding the values down to the nearest 10 ticks.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Date Times

Applies to: `DateTime`, `DateTimeOffset`

Date times are serialized as required UTC values with microsecond precision by default and must be deserialized from required date time values.

Since `DateTime` values have an associated `DateTimeKind`, which is one of `Unspecified`, `Utc` or `Local`, conversion to UTC can be ambiguous. Default serialization does not make any assumptions or do any implicit conversions, so any `DateTime` values that are not defined with `DateTimeKind.Utc` will result in a `SerializationException`.

`DateTimeOffset` values always map to a specific instant in time, so can always be converted to UTC in an unambiguous way. During serialization, `DateTimeOffset` values will be converted to their UTC equivalent. This means that the offset information is lost, but the serialized value is guaranteed to identify the same instant in time.

Both `DateTime` and `DateTimeOffset` use 'ticks' as their base unit, where each tick represents 100 nanoseconds. Since the default precision is microseconds, serialization results in a slight truncation, equivalent to rounding the values down to the nearest 10 ticks.

The following settings can be used to customize serialization of `DateTime` values via the `[<ParquetDateTime>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Unit` | `TimeUnit` | `Microseconds` | Allows the value to be serialized as a millisecond or nanosecond precision date time instead of the default microsecond precision and allows deserialization from date times with these precisions. Serialization with millisecond precision will result in truncation. |
| `Optional` | `bool` | `false` | Allows the value to be serialized as an optional value and allows it to be deserialized from an optional value. Since this type is non-nullable, if a null value is encountered during deserialization then a `SerializationException` will be raised. |

<sub>[[Return to top]](#parquetfsharp)</sub>

### Strings

Applies to: `string`

Despite being reference types and having null as a valid value, strings are serialized as required values by default and must be deserialized from required values. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating strings as required provides a guarantee that any serialized values are not null. If a null value is encountered during serialization or deserialization, a `SerializationException` will be raised.

The following settings can be used to customize serialization of string values via the `[<ParquetString>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the value to be serialized as an optional value and allows it to be deserialized from an optional value. Any null values encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null values to be serialized and deserialized. This setting has no effect unless the value has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

### Byte Arrays

Applies to: `byte[]`

Byte arrays are not treated the same as other array types since Parquet has native support for them. This means that instead of being treated as repeated values they are treated as atomic values.

Despite being reference types and having null as a valid value, byte arrays are serialized as required values by default and must be deserialized from required values. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating byte arrays as required provides a guarantee that any serialized values are not null. If a null value is encountered during serialization or deserialization, a `SerializationException` will be raised.

The following settings can be used to customize serialization of byte array values via the `[<ParquetByteArray>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the value to be serialized as an optional value and allows it to be deserialized from an optional value. Any null values encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null values to be serialized and deserialized. This setting has no effect unless the value has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

### Lists & Arrays

Applies to: `'Element list`, `'Element[]`, `ResizeArray<'Element>`

Sequences of values are stored as [Parquet lists](https://github.com/apache/parquet-format/blob/4b1c72c837bec5b792b2514f0057533030fcedf8/LogicalTypes.md#lists), which contain repeated elements, analagous to the `'Element seq` or  `IEnumerable<'Element>` .NET types.

Supported sequence types are serialized as required Parquet lists by default, even for sequences that allow null as a valid value. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types. Treating sequences as required provides a guarantee that any serialized sequences are not null. If a null sequence is encountered during serialization or deserialization, a `SerializationException` will be raised.

The following settings can be used to customize serialization of sequences via the `[<ParquetList>]`, `[<ParquetArray1d>]` or `[<ParquetResizeArray>]` attributes depending on the sequence type:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the sequence to be serialized as an optional Parquet list and allows it to be deserialized from an optional list. Any null sequences encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null sequences to be serialized and deserialized. This setting has no effect unless the sequence has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

### Records

Applies to: `'FSharpRecord`

As well as allowing sequences of values, Parquet allows arbitrary nesting of fields through records (also called structs). During serialization, these records are deconstructed into columns - 'shredded' in Parquet terminology - one column for each field. Information about the record nesting structure is also captured and stored alongside the columnar values, which allows records to be re-constructed during deserialization - 'assembled' in Parquet terminology. More information on how this works can be found in Google's [Dremel paper](https://research.google.com/pubs/archive/36632.pdf), on which Parquet is based.

Support for arbitrarily nested data is made possible through serialization support for F# record types. Record types do not allow null as a valid value in F#, so are serialized as required records by default and must be deserialized from required records. Any null values encountered during serialization or deserialization will result in a `SerializationException`. Records using the `[<Struct>]` attribute are supported in addition to standard (reference-type) records, and are serialized in exactly the same way. Mutable record fields are also supported.

One of the advantages of Parquet being a columnar data format is that it's possible and efficient to load only a subset of columns, and this behaviour is supported in **Parquet.FSharp**. When deserializing records, any fields in the Parquet file that are not specified in the target F# record type will be skipped. This enables a degree of forwards compatability - if new fields are added to the schema, it will still possible to deserialize using the old F# record types.

The following settings can be used to customize serialization of standard (reference-type) records via the `[<ParquetRecord>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the record to be serialized as an optional Parquet record and allows it to be deserialized from an optional record. Any null records encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null records to be serialized and deserialized. This setting has no effect unless the record has been configured as `Optional`. |

The following settings can be used to customize serialization of `[<Struct>]` (value-type) records via the `[<ParquetRecordStruct>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the record to be serialized as an optional Parquet record and allows it to be deserialized from an optional record. Since this type is non-nullable, if a null value is encountered during deserialization then a `SerializationException` will be raised. |

<sub>[[Return to top]](#parquetfsharp)</sub>

### Optional Types

Applies to: `'Value option`, `'Value voption`, `Nullable<'Value>`

Parquet supports both optional and required values. In F#, nullable values are not an idiomatic way to represent optionality - the preferred alternative being option types `'Value option` and `'Value voption`, or in some cases the `Nullable<'Value>` type. This makes optional values much more explicit and discoverable.

Due to the above, the default approach for serialization of other supported types is to treat the values as required, even if they are implicitly nullable through being a reference type. This helps prevent null values from appearing when they aren't expected. Values can instead be serialized as optional values by wrapping them in one of the supported optional types.

Optional types can be deserialized from both optional and required values. When deserailzied from required values, they are guaranteed to have an associated value and will therefore never be 'null'.

Note that Parquet does not support multiple levels of optionality, so nested optional types such as `'Value option option` are not supported. Attempting to serialize nested optional values will result in a `SerializationException`. Instead, the recommended approach for handling nested optional values is to add another level of nesting using an optional record containing a single optional field, for example:

```fsharp
// Nested options are not allowed by the Parquet format.
type IntOptionOption = int option option

// Instead, they can be represented using an optional
// record with an optional field value.
type IntOption = { Value: int option }
type IntOptionOption = IntOption option
```

<sub>[[Return to top]](#parquetfsharp)</sub>

### Discriminated Unions

Applies to: `'FSharpUnion`

Discriminated unions can be used to represent a range of different types with varying complexity. Complex unions require a more flexible - and therefore more complex - serialization approach. Even though all unions _could_ be serialized using this same flexible approach, it becomes fairly cumbersome and verbose for simpler unions. For this reason, **Parquet.FSharp** defines several different categories of union, each of which has a distinct use-case and is serialized in a different way.

<sub>[[Return to top]](#parquetfsharp)</sub>

#### Enumeration Unions

The simplest type of union is one in which there are no associated data fields for any of the cases. This gives a type that's very similar to an enum but without explicit backing values. An example is as follows:

```fsharp
type Shape =
    | Circle
    | Triangle
    | Square
    | Rectangle
```

Since none of the cases has any associated data fields, there is no additional nesting required to represent these union values. By default, enum unions are serialized as required string values and must be deserialized from required string values. Unions do not allow null as a valid value in F#, so any null values encountered during serialization or deserialization will result in a `SerializationException`.

The following settings can be used to customize serialization of enum unions via the `[<ParquetEnumUnion>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the union to be serialized as an optional string value and allows it to be deserialized from an optional string value. Any null unions encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null unions to be serialized and deserialized. This setting has no effect unless the union has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

#### Single-Case Unions

Unions with a single case are treated separately from unions with multiple cases because they don't distinguish between two equivalent types. They are often used to wrap primitive data values to create a richer set of domain types and to provide encapsulation - see [F# For Fun and Profit - Designing with types: Single case union types](https://fsharpforfunandprofit.com/posts/designing-with-types-single-case-dus/). Some examples are shown below:

```fsharp
type Age = Age of age:int
type Name = Name of firstName:string * lastName:string
type EmailAddress = EmailAddress of string
```

Since there is only a single case, there is no need to store the case name so only the fields are serialized. Single-case unions are serialized as required records, with one field for each associated data field. For example, the types above are serialized as if they were the following equivalent record types:

```fsharp
type Age = { age: int }
type Name = { firstName: string; lastName: string }

// Default field name assigned by F# compiler used, as none specified in type definition.
type EmailAddress = { Item1: string }
```

Single-case unions must be deserialized from required record values containing the correct field definitions. Unions do not allow null as a valid value in F#, so any null values encountered during serialization or deserialization will result in a `SerializationException`.

The following settings can be used to customize serialization of single-case unions via the `[<ParquetSingleCaseUnion>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the union to be serialized as an optional record and allows it to be deserialized from an optional record. Any null unions encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null unions to be serialized and deserialized. This setting has no effect unless the union has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

#### Multi-Case Unions

Unions with multiple cases where at least one case has one or more associated data fields are the most complex category of union and therefore require the most flexible approach. Some examples are shown below, taken from [Microsoft Learn - F# Discriminated Unions](https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/discriminated-unions):

```fsharp
type Shape =
    | Rectangle of width:float * length:float
    | Circle of radius:float
    | Prism of width:float * height:float * length:float

type BinaryTree =
    | Leaf
    | Node of value:int * left:BinaryTree * right:BinaryTree
```

Since there are multiple cases, the case name is serialized alongside the data fields as a string value. Like single-case unions, the case data fields are serialized as a record. However, since each case contains its own distinct set of fields with different names and types, each is serialized into its own independent record structure. The complete schema for a multi-case union consists of a required outer record containing a required string field `Type` for the case name and one or more case data fields. Each case data field is itself a record, containing any associated data fields. Since only one case will have data for any given union value, the case data fields are optional. The following demonstrates the equivalent serialization structures for the union types above:

```fsharp
type Shape = {
    Type: string
    Rectangle: Rectangle option
    Circle: Circle option
    Prism: Prism option }
and Rectangle = { width: float; length: float }
and Circle = { radius: float }
and Prism = { width: float; height: float; length: float }

type BinaryTree = {
    Type: string
    Node: Node option }
and Node = { value: int; left: BinaryTree; right: BinaryTree }
// The 'Leaf' case has no data fields so no inner record is required.
```

Multi-case unions must be deserialized from required record values containing the correct structure. Unions do not allow null as a valid value in F#, so any null values encountered during serialization or deserialization will result in a `SerializationException`.

The following settings can be used to customize serialization of multi-case unions via the `[<ParquetMultiCaseUnion>]` attribute:

| Setting | Type | Default | Description |
|-|-|-|-|
| `Optional` | `bool` | `false` | Allows the union to be serialized as an optional record and allows it to be deserialized from an optional record. Any null unions encountered during serialization or deserialization will still result in a `SerializationException` unless this behaviour is explicitly overriden using the `AllowNull` setting. |
| `AllowNull` | `bool` | `false` | Allows null unions to be serialized and deserialized. This setting has no effect unless the union has been configured as `Optional`. |

<sub>[[Return to top]](#parquetfsharp)</sub>

## Customization

**Parquet.FSharp** supports a wide range of commonly used types and provides sensible defaults for serialization, but there are cases when it's useful to extend and/or override this default behaviour. This customization is currently achieved through use of attributes, which allow default serialization settings to be overriden for fields and values.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Field Settings

| Setting | Type | Default | Description |
|-|-|-|-|
| `Name` | `string option` | `Option.None` | Allows the name of the target field to be overriden. |

Field settings can be customized using attributes derived from the `ParquetFieldSettingsAttribute` abstract class. **Parquet.FSharp** provides a single derived attribute `[<ParquetField>]`, which can be used as follows:

```fsharp
type Event = {
    [<ParquetField(Name = "EventId")>]
    Id: int64
    [<ParquetField(Name = "EventType")>]
    Type: string }
```

<sub>[[Return to top]](#parquetfsharp)</sub>

### Value Settings

| Setting | Type | Default | Description |
|-|-|-|-|
| `Converter` | `IValueConverter option` | `Option.None` | Allows a converter instance to be specified for serialization of the target value. If not specified, the default converter for the target value type will be used. See [Supported Types](#supported-types) for more information on this default behaviour. |

Value settings can be customized using attributes derived from the `ParquetValueSettingsAttribute` abstract class. **Parquet.FSharp** defines derived attributes for each of the built-in value converters, allowing the settings of these converters to be customized. Information on the attributes and converter settings available can be found in [Supported Types](#supported-types). Some examples are shown below:

```fsharp
// Always serialize this record as if it was an optional value rather than a
// required value. Any null values encountered will still raise an exception.
[<ParquetRecord(Optional = true)>]
type DataSample = {
    // Override the date time precision from the default of microseconds.
    [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>]
    Time: DateTime
    // Serialize as an optional string and explicitly allow null values.
    [<ParquetString(Optional = true, AllowNull = true)>]
    Type: string
    // No attribute so default float converter will be used.
    Value: float }
```

There are cases where values can be nested inside other (generic) types, primarily for optional and sequence types. In these situations you may want to customize the behaviour of both the parent and child types. To allow configuration to an arbitrary nesting depth, the `ParquetValueSettingsAttribute` base class defines an integer `NestingLevel` property. This property defaults to a value of zero indicating no nesting, i.e. the attribute is applied to the target field type. When set to a value greater than zero, the attribute is applied to the type at the specified nesting level, as shown in the following (fairly contrived!) examples:

```fsharp
type Task = {
    Id: Guid
    Description: string
    // Treat the option type as required rather than optional.
    // Override the precision of the nested date time value.
    [<ParquetOption(Required = true)>]
    [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
    DueAt: DateTime option
    // Treat the inner list as optional.
    // Treat the string value as optional and allow nulls.
    [<ParquetList(NestingLevel = 1, Optional = true)>]
    [<ParquetString(NestingLevel = 2, Optional = true, AllowNull = true)>]
    TagsGroups: string list list }
```

<sub>[[Return to top]](#parquetfsharp)</sub>

## Roadmap

The following features and improvements are on the roadmap and may be implemented in the future, in no particular order.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Extend Supported Types

The following types are not currently supported but will likely be added in the future:

- `DateOnly`
- `TimeOnly`
- `Interval` (from **Parquet.Net**)
- `BigDecimal` (from **Parquet.Net**)
- `BigInteger` (from **System.Numerics**)
- Tuples
- `'Element seq`
- `Map<'Key, 'Value>`
- `Dictionary<'Key, 'Value>`
- Classes

<sub>[[Return to top]](#parquetfsharp)</sub>

### Extend Attribute Support

Attributes can already be used to control serialization behaviour, but there are several more options that could be made available for built-in converters, for example the ability to:

- Override union case names.
- Treat date time values as local rather than UTC.
- Specify the precision and scale of decimal values.
- Ignore certain fields within a record.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Serialization Options

The ability to specify options at the serialization call site to allow finer-grained control of serialization behaviour. This would essentially allow the same set of configuration options currently provided by attributes to be configured dynamically at serialization time rather than at compile time.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Custom Converters

Allow the definition of custom converters that can be used to override the default serialization behaviour provided by the library for specific types, or allow serialization of types that aren't supported by the library. The default serialization behaviour defined in the library is already set up in this way, but the list of registered converters is not yet configurable and just contains a default converter for each supported type.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Improved Error Handling

For performance reasons, serialization is implemented by generating, compiling and executing [Expression Trees](https://learn.microsoft.com/en-us/dotnet/csharp/advanced-topics/expression-trees/), in a similar fashion to **Parquet.Net**. This means that errors are not always easy to trace back to the code that caused them. To improve on this, extra exception handling could be added into the generated expression trees to provide more information about the expression that was being executed when the exception occurred, e.g. to identify the specific converter function that was being called.

<sub>[[Return to top]](#parquetfsharp)</sub>

### Schema Evolution

Sometimes schema changes are required, and in some cases this could be done in a backwards compatible way, i.e. without breaking deserialization of Parquet files using the old schema. The following backward compatible changes could be allowed in future:

- Adding optional record fields
- Adding union cases
- Adding optional union case fields

<sub>[[Return to top]](#parquetfsharp)</sub>
