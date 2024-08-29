using BlockParty.Blocks.Filter;
using System;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<T>
    {
        private readonly ISourceBlock<T> _inputBlock;
        private IPropagatorBlock<T, T> _lastBlock;
        private readonly BufferBlock<T> _outputBlock = new BufferBlock<T>();

        private static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        public DataflowBuilder(ISourceBlock<T> sourceBlock)
        {
            _inputBlock = sourceBlock;
        }

        public DataflowBuilder<T> Where(FliterBlock<T>.Predicate predicate)
        {
            var newBlock = new FliterBlock<T>(predicate);

            if (_lastBlock == null)
            {
                _lastBlock = newBlock;
                _inputBlock.LinkTo(_lastBlock, _linkOptions);
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
                _lastBlock = newBlock;
            }

            return this;
        }

        public DataflowBuilder<TOutput> Select<TOutput>(Func<T, TOutput> lambda)
        {
            var newBlock = new TransformBlock<T, TOutput>(lambda);

            if (_lastBlock == null)
            {
                _inputBlock.LinkTo(newBlock, _linkOptions);
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
            }

            return new DataflowBuilder<TOutput>(newBlock);
        }

        public BufferBlock<T> Build()
        {
            if (_lastBlock == null)
            {
                _inputBlock.LinkTo(_outputBlock, _linkOptions);
            }
            else
            {
                _lastBlock.LinkTo(_outputBlock, _linkOptions);
            }
            
            return _outputBlock;
        }
    }
}