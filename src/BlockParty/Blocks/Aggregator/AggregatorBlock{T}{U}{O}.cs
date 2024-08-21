using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.Aggregator
{
    public class AggregatorBlock<T, U, O> : IPropagatorBlock<T, O>, IReceivableSourceBlock<O>
    {
        private readonly ITargetBlock<T> m_target;
        private readonly IReceivableSourceBlock<O> m_source;

        public AggregatorBlock(IPropagatorBlock<T, U> innerBlock, Func<T, U, O> aggregatorLambda)
        {
            var linkOptions = new DataflowLinkOptions()
            {
                PropagateCompletion = true,
            };
            var inputBlock = new BroadcastBlock<T>(input => input);
            var outputBlock = new BufferBlock<O>();

            var joinBlock = new JoinBlock<T, U>();
            var aggregatorBlock = new TransformBlock<Tuple<T, U>, O>(inputOutputTuple =>
            {
                var (input, output) = inputOutputTuple;
                return aggregatorLambda(input, output);
            });

            inputBlock.LinkTo(innerBlock, linkOptions);
            inputBlock.LinkTo(joinBlock.Target1, linkOptions);
            innerBlock.LinkTo(joinBlock.Target2, linkOptions);
            joinBlock.LinkTo(aggregatorBlock, linkOptions);
            aggregatorBlock.LinkTo(outputBlock, linkOptions);

            m_target = inputBlock;
            m_source = outputBlock;
        }

        public Task Completion { get { return m_source.Completion; } }

        public void Complete()
        {
            m_target.Complete();
        }

        public O ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<O> target, out bool messageConsumed)
        {
            return m_source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            m_target.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<O> target, DataflowLinkOptions linkOptions)
        {
            return m_source.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T> source, bool consumeToAccept)
        {
            return m_target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<O> target)
        {
            m_source.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<O> target)
        {
            return m_source.ReserveMessage(messageHeader, target);
        }

        public bool TryReceive(Predicate<O> filter, out O item)
        {
            return m_source.TryReceive(filter, out item);
        }

        public bool TryReceiveAll(out IList<O> items)
        {
            var reuslt = m_source.TryReceiveAll(out items);
            items = items ?? new List<O>();
            return reuslt;
        }
    }
}