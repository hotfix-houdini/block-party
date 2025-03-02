using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;

public class PropagatorNode // todo just work with this as a "node"
{
    public required int Id;
    public required Type InputType;
    public required Type OutputType; // todo make potentially optional for action blocks 
    public required IDataflowBlock Block;

    public List<PropagatorNode> Children { get; set; } = new List<PropagatorNode>();

    public string Name()
    {
        var blockName = Block.GetType().Name;
        return blockName.Substring(0, blockName.IndexOf('`'));
    }

    public static PropagatorNode Create<TInput, TOutput>(IDataflowBlock block, int id)
    {
        return new PropagatorNode()
        {
            Id = id,
            Block = block,
            InputType = typeof(TInput),
            OutputType = typeof(TOutput)
        };
    }
}