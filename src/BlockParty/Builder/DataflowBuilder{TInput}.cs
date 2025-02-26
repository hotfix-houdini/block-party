using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder;

/// <summary>
/// Used to construct encapsulated block chains. <br/><br/>
/// Example: `new DateFlowBuilder<int>().Filter(n => n % 2 == 0).Transform(n => $"{n + 1}").Build()`, which makes a pipeline to filter to even numbers, then map to +1 as a string.
/// 
/// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details, examples, and tests.
/// </summary>
public class DataflowBuilder<TInput> : DataflowBuilder<TInput, TInput>
{
    public DataflowBuilder() : base(new BufferBlock<TInput>())
    {
        _lastBlock = _sourceBlock;
    }
}
