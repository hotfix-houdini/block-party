using BlockParty.Blocks.Filter;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TOriginalType, TCurrentType>
    {
        protected readonly BufferBlock<TOriginalType> _sourceBlock;
        protected ISourceBlock<TCurrentType> _lastBlock;

        protected static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        public DataflowBuilder()
            : this(new BufferBlock<TOriginalType>(), null)
        {
        }

        public DataflowBuilder(BufferBlock<TOriginalType> originalBlock, ISourceBlock<TCurrentType> currentBlock)
        {
            _sourceBlock = originalBlock;
            _lastBlock = currentBlock;
        }

        public DataflowBuilder<TOriginalType, TNewType> Select<TNewType>(Func<TCurrentType, TNewType> lambda)
        {
            var newBlock = new TransformBlock<TCurrentType, TNewType>(lambda);
            AddBlock(newBlock);
            return new DataflowBuilder<TOriginalType, TNewType>(_sourceBlock, newBlock);
        }

        public DataflowBuilder<TOriginalType, TCurrentType> Where(FliterBlock<TCurrentType>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TCurrentType>(predicate);
            AddBlock(newBlock);
            return this;
        }

        public IPropagatorBlock<TOriginalType, TCurrentType> Build()
        {
            return DataflowBlock.Encapsulate(_sourceBlock, _lastBlock);
        }

        protected virtual void AddBlock<TNewType>(IPropagatorBlock<TCurrentType, TNewType> newBlock)
        {
            if (_lastBlock == null)
            {
                // Handle the case where the chain starts with the source block
                var castedBlock = new TransformBlock<TOriginalType, TCurrentType>(x => (TCurrentType)(object)x);
                _sourceBlock.LinkTo(castedBlock, _linkOptions);
                castedBlock.LinkTo(newBlock, _linkOptions);
            }
            else
            {
                // Link the last block to the new block
                _lastBlock.LinkTo(newBlock, _linkOptions);
            }

            // Update the last block to be the new block
            _lastBlock = newBlock as ISourceBlock<TCurrentType>;
        }
    }

    // Entry point for the builder
    public class DataflowBuilder<T> : DataflowBuilder<T, T>
    {
        public DataflowBuilder() : base()
        {
        }

        public new IPropagatorBlock<T, T> Build()
        {
            if (_lastBlock == null)
            {
                return _sourceBlock;
            }
            else
            {
                return DataflowBlock.Encapsulate(_sourceBlock, _lastBlock);
            }
        }
    }
}
