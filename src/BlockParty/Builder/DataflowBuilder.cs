using BlockParty.Blocks.Filter;
using System;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TCurrentType>
    {
        protected readonly BufferBlock<TCurrentType> _sourceBlock;
        protected IPropagatorBlock<TCurrentType, TCurrentType> _lastBlock;

        protected static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        public DataflowBuilder() : this(new BufferBlock<TCurrentType>())
        {
        }

        protected DataflowBuilder(BufferBlock<TCurrentType> originalBlock)
        {
            _sourceBlock = originalBlock;
        }

        public DataflowBuilder<TCurrentType> Where(FliterBlock<TCurrentType>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TCurrentType>(predicate);
            AddBlock(newBlock);
            return this;
        }

        public DataflowBuilder<TCurrentType> Select(Func<TCurrentType, TCurrentType> lambda)
        {
            var newBlock = new TransformBlock<TCurrentType, TCurrentType>(lambda);
            AddBlock(newBlock);
            return this;
        }

        public IPropagatorBlock<TCurrentType, TCurrentType> Build()
        {
            if (_lastBlock == null)
            {
                return _sourceBlock;
            }
            else
            {
                return DataflowBlock.Encapsulate<TCurrentType, TCurrentType>(_sourceBlock, _lastBlock);
            }
        }

        private void AddBlock(IPropagatorBlock<TCurrentType, TCurrentType> newBlock)
        {
            if (_lastBlock == null)
            {
                _lastBlock = newBlock;
                _sourceBlock.LinkTo(_lastBlock, _linkOptions);
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
                _lastBlock = newBlock;
            }
        }
    }
}