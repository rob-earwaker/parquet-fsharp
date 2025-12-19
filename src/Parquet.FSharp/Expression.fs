module [<AutoOpen>] internal Parquet.FSharp.ExpressionExtensions
// TODO: Should eventually be able to move this below TypeInfo.fs if we end up
// moving all expression code into the Shredder and Assembler modules.

open System
open System.Linq.Expressions

type Expression with
    static member Null(dotnetType) =
        Expression.Constant(null, dotnetType)
        :> Expression

    static member False = Expression.Constant(false) :> Expression
    static member True = Expression.Constant(true) :> Expression

    static member IsNull(value: Expression) =
        Expression.Equal(value, Expression.Null(value.Type))
        :> Expression

    static member OrElse([<ParamArray>] values: Expression[]) =
        values
        |> Array.reduce (fun combined value ->
            Expression.OrElse(combined, value))

    static member AndAlso([<ParamArray>] values: Expression[]) =
        values
        |> Array.reduce (fun combined value ->
            Expression.AndAlso(combined, value))

    static member Call(instance: Expression, methodName: string, arguments) =
        Expression.Call(instance, methodName, [||], Array.ofSeq arguments)
        :> Expression

    static member FailWith(message: string) =
        Expression.Throw(Expression.Constant(exn(message)))
        :> Expression
