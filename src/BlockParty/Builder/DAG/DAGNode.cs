using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;

public record DAGNode
{
    private readonly int _id;
    private readonly Type _inputType;
    private readonly Type _outputType;
    private readonly IDataflowBlock _block;

    private DAGNode(int id, Type inputType, Type outputType, IDataflowBlock block)
    {
        _id = id;
        _inputType = inputType;
        _outputType = outputType;
        _block = block;
    }

    public List<DAGNode> Children { get; set; } = new List<DAGNode>();

    public string Name()
    {
        var potentiallyTypedName = _block.GetType().Name;
        var withoutTypeParameterCountName = potentiallyTypedName[..potentiallyTypedName.IndexOf('`')];
        return withoutTypeParameterCountName;
    }

    public static DAGNode Create<TInput, TOutput>(IDataflowBlock block, int id)
    {
        return new DAGNode(id, typeof(TInput), typeof(TOutput), block);
    }

    public string IndexedNodeName()
    {
        return $"{ToCamelCase(this.Name()).Replace("Block", "")}_{this._id}";
    }

    private string ToCamelCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        var result = new StringBuilder(str);
        result[0] = char.ToLower(result[0]);

        return result.ToString();
    }

    public string ConstructTypeName()
    {
        return ConstructTypeNameRecursively(_block.GetType());
    }

    private static string ConstructTypeNameRecursively(Type type)
    {
        if (!type.IsGenericType)
            return type.Name;

        var typeName = type.Name.Substring(0, type.Name.IndexOf('`'));
        var typeArguments = type.GetGenericArguments()
                                .Select(ConstructTypeNameRecursively); // Recursively process generic arguments

        return $"{typeName}&lt;{string.Join(",", typeArguments)}&gt;";
    }
}