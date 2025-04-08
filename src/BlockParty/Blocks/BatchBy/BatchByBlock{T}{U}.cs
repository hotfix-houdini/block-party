using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.BatchBy;

/// <summary>
/// BatchByBlock is similar to the BatchBlock where TIn gets converted into TIn[] as the output. But, instead of just batching by a number of items, we batch by an arbitrary groupKey via a lambda. 
/// This is similar to a GroupBy in LINQ, but it does not run the GroupBy on the entire datafow. Instead, it only groups contiguous items. When a different group is received, the previous batch is emitted. <br/><br/>
/// 
/// For example, this block could let you group contiguous items into a larger array by a common id.
/// Or you can group items into arbitrary time windows. <br/><br/>
/// 
/// The `selector` parameter is the lambda you use to map an item to it's group. It must implement `IEquatable` of itself.<br/><br/>
/// 
/// This block (and nearly all in this repo) only supports in-order non-parallel processing.<br/>
/// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
/// </summary>
public class BatchByBlock<TItem, UGroup> : IPropagatorBlock<TItem, TItem[]>, IReceivableSourceBlock<TItem[]>
    where UGroup : IEquatable<UGroup>
{
    private readonly ITargetBlock<TItem> m_target;
    private readonly IReceivableSourceBlock<TItem[]> m_source;

    /// <summary>
    /// BatchByBlock is similar to the BatchBlock where TIn gets converted into TIn[] as the output. But, instead of just batching by a number of items, we batch by an arbitrary groupKey via a lambda. 
    /// This is similar to a GroupBy in LINQ, but it does not run the GroupBy on the entire datafow. Instead, it only groups contiguous items. When a different group is received, the previous batch is emitted. <br/><br/>
    /// 
    /// For example, this block could let you group contiguous items into a larger array by a common id.
    /// Or you can group items into arbitrary time windows. <br/><br/>
    /// 
    /// The `selector` parameter is the lambda you use to map an item to it's group. It must implement `IEquatable` of itself.<br/><br/>
    /// 
    /// This block (and nearly all in this repo) only supports in-order non-parallel processing.<br/>
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
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
