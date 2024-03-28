using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.Beam
{
    /// <summary>
    /// BeamBlock is inspired by the Apache Beam stream processing framework. It allows you to aggregate a stream of T into windowed accumulators.<br/><br/>
    /// 
    /// For example, if you want to create candlesticks from a stream of stock prices, this block will enable you to:<br/>
    ///     - group by a specific TimeSpan window size (you can select the Time property from your model)<br/>
    ///     - accumulate all the records in the window using your own logic (such as calculating max, min, volume)<br/>
    ///     - emit the accumulated window once the window has passed.<br/><br/>
    /// 
    /// This block can support aggregating past streams, in addition to real-time streams.<br/><br/>
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
    public class BeamBlock<TStreamedModel, TAccumulator>
        : IPropagatorBlock<TStreamedModel, TAccumulator>, IReceivableSourceBlock<TAccumulator>
        where TAccumulator : class, IAccumulator, new()
    {
        private readonly ITargetBlock<TStreamedModel> m_target;
        private readonly IReceivableSourceBlock<TAccumulator> m_source;

        /// <summary>
        /// BeamBlock is inspired by the Apache Beam stream processing framework. It allows you to aggregate a stream of T into windowed accumulators.<br/><br/>
        /// 
        /// For example, if you want to create candlesticks from a stream of stock prices, this block will enable you to:<br/>
        ///     - group by a specific TimeSpan window size (you can select the Time property from your model)<br/>
        ///     - accumulate all the records in the window using your own logic (such as calculating max, min, volume)<br/>
        ///     - emit the accumulated window once the window has passed.<br/><br/>
        /// 
        /// This block can support aggregating past streams, in addition to real-time streams.<br/><br/>
        /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
        /// </summary>
        public BeamBlock(
            TimeSpan window,
            Action<TStreamedModel, TAccumulator> accumulateMethod,
            Func<TStreamedModel, long> timeSelectionMethod)
        {
            var nanosecondWindow = window.Ticks * 100;

            var accumulator = new TAccumulator();
            long currentWindow = -1;
            var initialized = false;

            var source = new BufferBlock<TAccumulator>();
            var target = new ActionBlock<TStreamedModel>(item =>
            {
                var itemTime = timeSelectionMethod(item);
                if (itemTime / nanosecondWindow > currentWindow)
                {
                    if (initialized)
                    {
                        var posted = source.Post(accumulator);
                        if (!posted)
                        {
                            throw new FailedToPostException();
                        }

                        var newWindow = itemTime / nanosecondWindow;
                        var missingWindows = newWindow - currentWindow - 1;
                        for (int i = 0; i < missingWindows; i++)
                        {
                            var windowStart = (currentWindow + i + 1) * nanosecondWindow;
                            var emptyWindow = new TAccumulator
                            {
                                WindowStart = windowStart,
                                WindowEnd = windowStart + nanosecondWindow
                            };
                            posted = source.Post(emptyWindow);
                            if (!posted)
                            {
                                throw new FailedToPostException();
                            }
                        }
                    }
                    else
                    {
                        initialized = true;
                    }

                    accumulator = new TAccumulator();
                    currentWindow = itemTime / nanosecondWindow;
                    accumulator.WindowStart = (itemTime / nanosecondWindow) * (nanosecondWindow);
                    accumulator.WindowEnd = accumulator.WindowStart + nanosecondWindow;
                }

                accumulateMethod(item, accumulator);
            });

            target.Completion.ContinueWith(delegate
            {
                // let the caller decide if it should keep the very last window
                if (initialized)
                {
                    var posted = source.Post(accumulator);
                    if (!posted)
                    {
                        throw new FailedToPostException();
                    }
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

        public TAccumulator ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<TAccumulator> target, out bool messageConsumed)
        {
            return m_source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            m_target.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<TAccumulator> target, DataflowLinkOptions linkOptions)
        {
            return m_source.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, TStreamedModel messageValue, ISourceBlock<TStreamedModel> source, bool consumeToAccept)
        {
            return m_target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<TAccumulator> target)
        {
            m_source.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<TAccumulator> target)
        {
            return m_source.ReserveMessage(messageHeader, target);
        }

        public bool TryReceive(Predicate<TAccumulator> filter, out TAccumulator item)
        {
            return m_source.TryReceive(filter, out item);
        }

        public bool TryReceiveAll(out IList<TAccumulator> items)
        {
            var reuslt = m_source.TryReceiveAll(out items);
            items = items ?? new List<TAccumulator>();
            return reuslt;
        }
    }
}
