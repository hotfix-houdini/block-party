using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;

public class TargetNode <TType>
{
    public required ITargetBlock<TType> TargetBlock { get; set; }
}