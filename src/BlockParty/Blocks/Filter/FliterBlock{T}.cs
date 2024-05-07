using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.Filter
{
    /// <summary>
    /// FliterBlock allows you to filter down a stream by any lambda method.<br/>
    /// 
    /// <param name="predicate">The lambda used to gatekeep the downstream.</param><br/><br/>
    /// 
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
    public class FliterBlock<T> : IPropagatorBlock<T, T>, IReceivableSourceBlock<T>
    {
        public delegate bool Predicate(T item);

        private readonly ITargetBlock<T> m_target;
        private readonly IReceivableSourceBlock<T> m_source;

        /// <summary>
        /// FliterBlock allows you to filter down a stream by any lambda method.<br/>
        /// 
        /// <param name="predicate">The lambda used to gatekeep the downstream.</param><br/><br/>
        /// 
        /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
        /// </summary>
        public FliterBlock(Predicate predicate)
        {
            var source = new BufferBlock<T>();
            var target = new ActionBlock<T>(item =>
            {
                if (predicate(item))
                {
                    var posted = source.Post(item);
                    if (!posted)
                    {
                        throw new FailedToPostException();
                    }
                }
            });

            target.Completion.ContinueWith(delegate
            {
                source.Complete();
            });

            m_target = target;
            m_source = source;
        }

        public Task Completion { get { return m_source.Completion; } }

        public void Complete()
        {
            m_target.Complete();
        }

        public T ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
        {
            return m_source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            m_target.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
        {
            return m_source.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T> source, bool consumeToAccept)
        {
            return m_target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
        {
            m_source.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
        {
            return m_source.ReserveMessage(messageHeader, target);
        }

        public bool TryReceive(Predicate<T> filter, out T item)
        {
            return m_source.TryReceive(filter, out item);
        }

        public bool TryReceiveAll(out IList<T> items)
        {
            var reuslt = m_source.TryReceiveAll(out items);
            items = items ?? new List<T>();
            return reuslt;
        }
    }
}
