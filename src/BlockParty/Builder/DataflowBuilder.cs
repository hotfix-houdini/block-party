using BlockParty.Blocks.Filter;
using System;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TSource>
    {
        private readonly ISourceBlock<TSource> _inputBlock; // necessary or nah?
        private IPropagatorBlock<TSource, TSource> _lastBlock;
        private BufferBlock<TSource> _outputBlock = new BufferBlock<TSource>();

        private static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        public DataflowBuilder(ISourceBlock<TSource> sourceBlock)
        {
            _inputBlock = sourceBlock;
        }

        public DataflowBuilder<TSource> Where(FliterBlock<TSource>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TSource>(predicate);

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

        public DataflowBuilder<TSource> Select(Func<TSource, TSource> lambda)
        {
            var newBlock = new TransformBlock<TSource, TSource>(upstream => lambda(upstream));

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

        public BufferBlock<TSource> Build()
        {
            if (_lastBlock == null)
            {
                throw new ArgumentException("Build() called without constructing a pipeline");
            }

            _lastBlock.LinkTo(_outputBlock, _linkOptions);
            return _outputBlock;

            //return DataflowBlock.Encapsulate<TSource, TSource>(_sourceBlock, _filterBlock1); // figure out if I SOURCE 
        }
    }
}
