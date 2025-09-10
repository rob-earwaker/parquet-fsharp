module Parquet.FSharp.Library

open System.Reflection

let private Assembly = lazy Assembly.GetExecutingAssembly()

let private InformationalVersion = lazy (
    Assembly.Value
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        .InformationalVersion)

let Name = lazy Assembly.Value.GetName().Name
let Version = lazy InformationalVersion.Value.Split('+')[0]
let Revision = lazy InformationalVersion.Value.Split('+')[1]
