using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.GroupBy;

public class BatchByBlock<TItem, UGroup> : IPropagatorBlock<TItem, TItem[]>, IReceivableSourceBlock<TItem[]>
    where UGroup : IEquatable<UGroup>
{
    private readonly ITargetBlock<TItem> m_target;
    private readonly IReceivableSourceBlock<TItem[]> m_source;

    // make note that the batchBy is a CONTIGUOUS operation
    public BatchByBlock(Func<TItem, UGroup> selector)
    {
        var groups = new Dictionary<UGroup, List<TItem>>();

        var source = new BufferBlock<TItem[]>();
        var target = new ActionBlock<TItem>(item =>
        {
            var groupKey = selector(item);

            var groupAlreadyExists = groups.TryGetValue(groupKey, out var existingGroup);
            if (!groupAlreadyExists)
            {
                var previousGroup = groups.Values.SingleOrDefault();
                if (previousGroup is not null)
                {
                    var posted = source.Post(previousGroup.ToArray());
                    if (!posted)
                    {
                        throw new FailedToPostException();
                    }
                    groups.Clear();
                }

                groups[groupKey] = [item];
            }
            else
            {
                existingGroup.Add(item);
            }
        });

        _ = target.Completion.ContinueWith(targetComplete =>
        {
            if (targetComplete.IsFaulted)
            {
                ((IDataflowBlock)source).Fault(targetComplete.Exception);
            }
            else if (targetComplete.IsCanceled)
            {
                ((IDataflowBlock)source).Fault(new TaskCanceledException());
            }
            else
            {
                if (groups.Any())
                {
                    var posted = source.Post(groups.Values.Single().ToArray());
                    if (!posted)
                    {
                        ((IDataflowBlock)source).Fault(new FailedToPostException());
                    }
                }
                source.Complete();
            }
        });

        m_target = target;
        m_source = source;
    }

    public Task Completion { get { return m_source.Completion; } }

    public void Complete()
    {
        m_target.Complete();
    }

    public TItem[] ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<TItem[]> target, out bool messageConsumed)
    {
        return m_source.ConsumeMessage(messageHeader, target, out messageConsumed);
    }

    public void Fault(Exception exception)
    {
        m_target.Fault(exception);
    }

    public IDisposable LinkTo(ITargetBlock<TItem[]> target, DataflowLinkOptions linkOptions)
    {
        return m_source.LinkTo(target, linkOptions);
    }

    public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, TItem messageValue, ISourceBlock<TItem> source, bool consumeToAccept)
    {
        return m_target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
    }

    public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<TItem[]> target)
    {
        m_source.ReleaseReservation(messageHeader, target);
    }

    public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<TItem[]> target)
    {
        return m_source.ReserveMessage(messageHeader, target);
    }

    public bool TryReceive(Predicate<TItem[]> filter, [MaybeNullWhen(false)] out TItem[] item)
    {
        return m_source.TryReceive(filter, out item);
    }

    public bool TryReceiveAll([NotNullWhen(true)] out IList<TItem[]> items)
    {
        var reuslt = m_source.TryReceiveAll(out items);
        items ??= [];
        return reuslt;
    }
}
