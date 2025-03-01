using System;
using System.Collections.Generic;
using System.Linq;

namespace BlockParty.Visualizer;

public record BlockNode2 // todo rename
{
    public required int Id { get; init; }
    public required string Name { get; init; }
    public required string InputType { get; init; }
    public string OutputType { get; init; }
    public required List<BlockNode2> Children { get; init; }

    public static BlockNode2 Create<TInput, TOutput>(string name, int id)
    {
        return BlockNode2.Create<TInput, TOutput>(name, id, new List<BlockNode2>());
    }

    public static BlockNode2 Create<TType>(string name, int id)
    {
        return BlockNode2.Create<TType>(name, id, new List<BlockNode2>());
    }

    public static BlockNode2 Create<TInput, TOutput>(string name, int id, List<BlockNode2> children)
    {
        return new BlockNode2()
        {
            Id = id,
            Name = name,
            InputType = ConstructTypeName<TInput>(),
            OutputType = ConstructTypeName<TOutput>(),
            Children = children
        };
    }

    public static BlockNode2 Create<TType>(string name, int id, List<BlockNode2> children)
    {
        return new BlockNode2()
        {
            Id = id,
            Name = name,
            InputType = ConstructTypeName<TType>(),
            OutputType = null,
            Children = children
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

        return $"{typeName}&lt;{string.Join(",&nbsp;", typeArguments)}&gt;";
    }
}