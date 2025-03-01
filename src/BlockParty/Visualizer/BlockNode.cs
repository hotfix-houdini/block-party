﻿using System;
using System.Linq;

namespace BlockParty.Visualizer;

public record BlockNode
{
    public required string Name { get; init; }
    public required string InputType { get; init; }
    public required string OutputType { get; init; }

    public static BlockNode Create<TInput, TOutput>(string name)
    {
        return new BlockNode()
        {
            Name = name,
            InputType = ConstructTypeName<TInput>(),
            OutputType = ConstructTypeName<TOutput>()
        };
    }

    private static string ConstructTypeName<TType>()
    {
        return ConstructTypeName(typeof(TType));
    }

    private static string ConstructTypeName(Type type)
    {
        if (!type.IsGenericType)
            return type.Name;

        var typeName = type.Name.Substring(0, type.Name.IndexOf('`'));
        var typeArguments = type.GetGenericArguments()
                                .Select(ConstructTypeName); // Recursively process generic arguments

        return $"{typeName}<{string.Join(", ", typeArguments)}>";
    }


    //private static string ConstructTypeName<TType>()
    //{
    //    var type = typeof(TType);
    //    var typeName = type.Name;
    //    if (type.IsGenericType)
    //    {
    //        var typeParameters = type.GetGenericArguments().ToList();
    //        typeName = type.Name.Substring(0, type.Name.IndexOf('`')) + "&lt;" +
    //            string.Join(",&nbsp;", Array.ConvertAll(type.GetGenericArguments(), t => t.Name)) + "&gt;";
    //    }

    //    return typeName;
    //}
}