using BlockParty.Blocks.Beam;
using BlockParty.Blocks.Filter;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder
{
    public class DataflowBuilder<TInput, TOutput>
    {
        protected readonly BufferBlock<TInput> _sourceBlock;
        protected ISourceBlock<TOutput> _lastBlock;

        private static readonly DataflowLinkOptions _linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };

        protected DataflowBuilder(BufferBlock<TInput> inputBlock)
            : this(inputBlock, null)
        {
        }

        private DataflowBuilder(BufferBlock<TInput> originalBlock, ISourceBlock<TOutput> currentBlock)
        {
            _sourceBlock = originalBlock;
            _lastBlock = currentBlock;
        }

        public DataflowBuilder<TInput, TNewType> Select<TNewType>(Func<TOutput, TNewType> lambda)
        {
            var newBlock = new TransformBlock<TOutput, TNewType>(lambda);
            AddBlock(newBlock);
            return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock);
        }

        public DataflowBuilder<TInput, TNewType> SelectMany<TNewType>(Func<TOutput, IEnumerable<TNewType>> lambda)
        {
            var newBlock = new TransformManyBlock<TOutput, TNewType>(lambda);
            AddBlock(newBlock);
            return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock);
        }

        public DataflowBuilder<TInput, TOutput> Where(FliterBlock<TOutput>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TOutput>(predicate);
            AddBlock(newBlock);
            return this;
        }

        public IPropagatorBlock<TInput, DoneResult> ForEachAndComplete(Action<TOutput> lambda)
        {
            var newBlock = new TransformBlock<TOutput, DoneResult>(input =>
            {
                lambda(input);
                return DoneResult.Instance;
            });
            AddBlock(newBlock);
            newBlock.LinkTo(DataflowBlock.NullTarget<DoneResult>());
            return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock).Build();
        }

        public IPropagatorBlock<TInput, DoneResult> ForEachAndComplete(Func<TOutput, Task> lambda)
        {
            var newBlock = new TransformBlock<TOutput, DoneResult>(async input =>
            {
                await lambda(input);
                return DoneResult.Instance;
            });
            AddBlock(newBlock);
            newBlock.LinkTo(DataflowBlock.NullTarget<DoneResult>());
            return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock).Build();
        }

        public DataflowBuilder<TInput, TAccumulator> Beam<TAccumulator>(
            TimeSpan window,
            Action<TOutput, TAccumulator> accumlateMethod,
            Func<TOutput, NanosecondTimeConverter, long> timeSelectionMethod,
            BeamBlockSettings settings = null) where TAccumulator : class, IAccumulator, new()
        {
            var newBlock = new BeamBlock<TOutput, TAccumulator>(window, accumlateMethod, timeSelectionMethod, settings);
            AddBlock(newBlock);
            return new DataflowBuilder<TInput, TAccumulator>(_sourceBlock, newBlock);
        }

        public IPropagatorBlock<TInput, TOutput> Build()
        {
            return DataflowBlock.Encapsulate(_sourceBlock, _lastBlock);
        }

        private void AddBlock<TNewType>(IPropagatorBlock<TOutput, TNewType> newBlock)
        {
            _lastBlock.LinkTo(newBlock, _linkOptions);
            _lastBlock = newBlock as ISourceBlock<TOutput>;
        }
    }
}