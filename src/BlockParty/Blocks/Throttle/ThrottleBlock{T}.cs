using BlockParty.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.Throttle;

/// <summary>
/// ThrottleBlock allows you to insert wall-clock delays into a TPL DataFlow pipeline.<br/>
/// Upstream blocks can be unthrottled. Downstream blocks will receieve a message once per internval.<br/><br/>
/// 
/// <param name="throttle">The throttle timespan must be >= 100 milliseconds due to .NET's time precision.</param><br/><br/>
/// 
/// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
/// </summary>
public class ThrottleBlock<T> : IPropagatorBlock<T, T>, IReceivableSourceBlock<T>
{
    private readonly ITargetBlock<T> m_target;
    private readonly IReceivableSourceBlock<T> m_source;
    private readonly ConcurrentQueue<T> _queue = new();
    private readonly Timer _timer;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _completitionRequested = false;
    private bool _fault = false;

    /// <summary>
    /// ThrottleBlock allows you to insert wall-clock delays into a TPL DataFlow pipeline.<br/>
    /// Upstream blocks can be unthrottled. Downstream blocks will receieve a message once per internval.<br/><br/>
    /// 
    /// <param name="throttle">The throttle timespan must be >= 100 milliseconds due to .NET's time precision.</param><br/><br/>
    /// 
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
    public ThrottleBlock(TimeSpan throttle)
    {
        if (throttle < TimeSpan.FromMilliseconds(100))
        {
            throw new ArgumentException("Period must be greater than or equal to 100 milliseconds.", nameof(throttle));
        }

        var source = new BufferBlock<T>();
        var target = new ActionBlock<T>(_queue.Enqueue);

        target.Completion.ContinueWith(targetComplete =>
        {
            if (targetComplete.IsFaulted)
            {
                ((IDataflowBlock)source).Fault(targetComplete.Exception);
                _fault = true;
            }
            else if (targetComplete.IsCanceled)
            {
                ((IDataflowBlock)source).Fault(new TaskCanceledException());
                _fault = true;
            }
            else
            {
                _completitionRequested = true;
            }
        });

        _timer = new Timer(async state =>
        {
            try
            {
                await _semaphore.WaitAsync();
                if (_fault)
                {
                    _timer?.Change(Timeout.Infinite, Timeout.Infinite);
                    return;
                }

                var dequeued = _queue.TryDequeue(out var item);
                if (dequeued)
                {
                    var posted = source.Post(item);
                    if (!posted)
                    {
                        throw new FailedToPostException();
                    }
                }
                else
                {
                    if (_completitionRequested)
                    {
                        source.Complete();
                        _timer?.Change(Timeout.Infinite, Timeout.Infinite);
                    }
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }, null, 0, Convert.ToInt32(throttle.TotalMilliseconds));

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
        items ??= [];
        return reuslt;
    }
}
