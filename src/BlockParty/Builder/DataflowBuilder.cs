using BlockParty.Blocks.Filter;
using System;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TCurrentType> : DataflowBuilder<TCurrentType, TCurrentType>
    {
        public DataflowBuilder() : base(new BufferBlock<TCurrentType>())
        {
        }

        public new DataflowBuilder<TCurrentType, TNewType> Select<TNewType>(Func<TCurrentType, TNewType> lambda)
        {
            var newBlock = new TransformBlock<TCurrentType, TNewType>(lambda);

            if (_lastBlock == null)
            {
                _sourceBlock.LinkTo(newBlock, _linkOptions);
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
            }

            return new DataflowBuilder<TCurrentType, TNewType>(_sourceBlock, newBlock);
        }

        public new IPropagatorBlock<TCurrentType, TCurrentType> Build()
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

        protected override void AddBlock(IPropagatorBlock<TCurrentType, TCurrentType> newBlock)
        {
            if (_lastBlock == null)
            {
                _sourceBlock.LinkTo(newBlock, _linkOptions);
                _lastBlock = newBlock;
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
                _lastBlock = newBlock;
            }
        }
    }

    public class DataflowBuilder<TOriginalType, TCurrentType>
    {
        protected readonly BufferBlock<TOriginalType> _sourceBlock;
        protected ISourceBlock<TCurrentType> _lastBlock;

        protected static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        public DataflowBuilder(BufferBlock<TOriginalType> originalBlock) : this(originalBlock, null)
        {
        }

        public DataflowBuilder(BufferBlock<TOriginalType> originalBlock, ISourceBlock<TCurrentType> currentBlock)
        {
            _sourceBlock = originalBlock;
            _lastBlock = currentBlock;
        }

        public DataflowBuilder<TOriginalType, TCurrentType> Where(FliterBlock<TCurrentType>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TCurrentType>(predicate);
            AddBlock(newBlock);
            return this;
        }

        public DataflowBuilder<TOriginalType, TCurrentType> Select(Func<TCurrentType, TCurrentType> lambda)
        {
            var newBlock = new TransformBlock<TCurrentType, TCurrentType>(lambda);
            AddBlock(newBlock);
            return this;
        }

        public DataflowBuilder<TOriginalType, TNewType> Select<TNewType>(Func<TCurrentType, TNewType> lambda)
        {
            var newBlock = new TransformBlock<TCurrentType, TNewType>(lambda);
            _lastBlock.LinkTo(newBlock, _linkOptions); // make addblock?
            return new DataflowBuilder<TOriginalType, TNewType>(_sourceBlock, newBlock);
        }

        public IPropagatorBlock<TOriginalType, TCurrentType> Build()
        {
            return DataflowBlock.Encapsulate<TOriginalType, TCurrentType>(_sourceBlock, _lastBlock);
        }

        protected virtual void AddBlock(IPropagatorBlock<TCurrentType, TCurrentType> newBlock)
        {
            if (_lastBlock == null)
            {
                _lastBlock = newBlock;
            }
            else
            {
                _lastBlock.LinkTo(newBlock, _linkOptions);
                _lastBlock = newBlock;
            }
        }
    }
}