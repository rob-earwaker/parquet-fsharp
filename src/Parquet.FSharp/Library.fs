module Parquet.FSharp.Library

open System.Reflection

let private Assembly = Assembly.GetExecutingAssembly()

let private InformationalVersion =
    Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        .InformationalVersion

let Name = Assembly.GetName().Name
let Version = InformationalVersion.Split('+')[0]
let Revision = InformationalVersion.Split('+')[1]
