using BlockParty.Blocks.Filter;
using System;
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

        public DataflowBuilder<TInput, TOutput> Where(FliterBlock<TOutput>.Predicate predicate)
        {
            var newBlock = new FliterBlock<TOutput>(predicate);
            AddBlock(newBlock);
            return this;
        }

        /* - introduce FanoutDataflowBuilder without
         *      - Where (b/c that would break the join)
         *      - Build (b/c we call that internally)
         * - make method slightly more generic (generic helper method to reduce duplicated code. can't loop though.)
         * - expose several overloads (2 chains, 3 chains, 4 chains, 5 chains)
         * - lots of complicated tests
         */
        public DataflowBuilder<TInput, (TNewType1, TNewType2)> FanoutWithJoin<TNewType1, TNewType2>(
            Func<DataflowBuilder<TOutput>, DataflowBuilder<TOutput, TNewType1>> chain1,
            Func<DataflowBuilder<TOutput>, DataflowBuilder<TOutput, TNewType2>> chain2)
        {
            var fanoutBlock = new BroadcastBlock<TOutput>(x => x);
            var joinBlock = new JoinBlock<TNewType1, TNewType2>();
            var tuple2ToValueTupleConverter = new TransformBlock<Tuple<TNewType1, TNewType2>, (TNewType1, TNewType2)>(x => (x.Item1, x.Item2));

            var newBuilder1 = new DataflowBuilder<TOutput>();
            var chain1Builder = chain1(newBuilder1);
            var chain1Block = chain1Builder.Build();

            var newBuilder2 = new DataflowBuilder<TOutput>();
            var chain2Builder = chain2(newBuilder2);
            var chain2Block = chain2Builder.Build();

            fanoutBlock.LinkTo(newBuilder1._sourceBlock, _linkOptions);
            fanoutBlock.LinkTo(newBuilder2._sourceBlock, _linkOptions);
            chain1Block.LinkTo(joinBlock.Target1, _linkOptions);
            chain2Block.LinkTo(joinBlock.Target2, _linkOptions);
            joinBlock.LinkTo(tuple2ToValueTupleConverter, _linkOptions);

            var newBlock = DataflowBlock.Encapsulate(fanoutBlock, tuple2ToValueTupleConverter);
            AddBlock(newBlock);
            return new DataflowBuilder<TInput, (TNewType1, TNewType2)>(_sourceBlock, newBlock);
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