using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TInput> : DataflowBuilder<TInput, TInput>
    {
        public DataflowBuilder() : base(new BufferBlock<TInput>())
        {
            _lastBlock = _sourceBlock;
        }
    }
}
