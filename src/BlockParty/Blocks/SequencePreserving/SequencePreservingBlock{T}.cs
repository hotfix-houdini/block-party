using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.SequencePreserving
{
    /// <summary>
    /// SequencePreservingBlock allows you to reorder streams when there's a known contiguous order. This can be useful when doing in-order processing in a pub/sub context where a message broker might not preserve the order.<br/>
    /// There is no watermark - messages before the initial sequence are dropped and non-contiguous messages can be buffered indefinitely. Beware of memory constraints.<br/><br/>
    /// 
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
    public class SequencePreservingBlock<T> : IPropagatorBlock<T, T>, IReceivableSourceBlock<T>
    {
        public delegate long SequenceIndexExtractor(T item);

        private readonly ITargetBlock<T> m_target;
        private readonly IReceivableSourceBlock<T> m_source;
        private readonly SequenceIndexExtractor _sequenceIndexExtractor;
        private readonly SequencePreservingBlockSettings _settings;
        private readonly SortedSet<T> _pendingMessages;
        
        private bool _sequenceSet = false;
        private long _sequenceNumber = 0;

        /// <summary>
        /// SequencePreservingBlock allows you to reorder streams when there's a known contiguous order. This can be useful when doing in-order processing in a pub/sub context where a message broker might not preserve the order.<br/>
        /// There is no watermark - messages before the initial sequence are dropped and non-contiguous messages can be buffered indefinitely. Beware of memory constraints.<br/><br/>
        /// 
        ///  <param name="sequenceIndexExtractor">- sequenceIndexExtractor is a lambda to select / construct the sequence number.</param><br/>
        ///  <param name="settings">- settings allows options for determining the initial sequence number and OnComplete behavior of buffered messages, if any.</param><br/><br/>
        ///  
        /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
        /// </summary>
        public SequencePreservingBlock(SequenceIndexExtractor sequenceIndexExtractor, SequencePreservingBlockSettings settings)
        {
            _sequenceIndexExtractor = sequenceIndexExtractor;
            _settings = settings;
            _pendingMessages = new SortedSet<T>(new SequenceIndexComparer(_sequenceIndexExtractor));

            if (!_settings.SequenceInitialization.FromFirstElementFlag)
            {
                _sequenceNumber = _settings.SequenceInitialization.InclusiveStartingSequenceNumber - 1;
                _sequenceSet = true;
            }

            var source = new BufferBlock<T>();
            var target = new ActionBlock<T>(item =>
            {
                _pendingMessages.Add(item);
                foreach (var pendingMessage in _pendingMessages.TakeWhile(message =>
                {
                    if (!_sequenceSet)
                    {
                        _sequenceNumber = _sequenceIndexExtractor(message);
                        _sequenceSet = true;
                        return true;
                    }
                    else if (_sequenceIndexExtractor(message) == _sequenceNumber + 1)
                    {
                        _sequenceNumber++;
                        return true;
                    }
                    return false;
                }))
                {
                    var posted = source.Post(pendingMessage);
                    if (!posted)
                    {
                        throw new FailedToPostException();
                    }
                }

                _pendingMessages.RemoveWhere(msg => _sequenceIndexExtractor(msg) <= _sequenceNumber);
            });

            target.Completion.ContinueWith(delegate
            {
                if (_settings.OnCompleteBufferedMessageBehavior == OnCompleteBufferedMessageBehavior.Emit)
                {
                    foreach (var message in _pendingMessages)
                    {
                        var posted = source.Post(message);
                        if (!posted)
                        {
                            throw new FailedToPostException();
                        }
                    }

                    _pendingMessages.RemoveWhere(x => true);
                }

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

        private class SequenceIndexComparer : IComparer<T>
        {
            private readonly SequenceIndexExtractor _sequenceIndexExtractor;

            public SequenceIndexComparer(SequenceIndexExtractor sequenceIndexExtractor)
            {
                _sequenceIndexExtractor = sequenceIndexExtractor;
            }

            public int Compare(T x, T y)
            {
                var sequenceIndexX = _sequenceIndexExtractor(x);
                var sequenceIndexY = _sequenceIndexExtractor(y);

                return sequenceIndexX.CompareTo(sequenceIndexY);
            }
        }
    }
}
