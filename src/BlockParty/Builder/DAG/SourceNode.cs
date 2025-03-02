using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;

public class SourceNode<TOutput>
{
    public required ISourceBlock<TOutput> SourceBlock { get; set; }
    //public TargetNode<TOutput> NextNode; // make list for fanouts
}