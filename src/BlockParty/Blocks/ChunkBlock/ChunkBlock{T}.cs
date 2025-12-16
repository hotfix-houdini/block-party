using BlockParty.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Blocks.ChunkBlock;

/// <summary>
/// ChunkBlock is similar to the BatchBlock where TIn gets converted into TIn[] as the output. But, instead of just batching by a number of items, we batch but some summation threshold. 
/// This is similar to the Chunk method in LINQ. When a chunk's sum exceeds the threshold, it will be emitted. An incomplete chunk will be emitted on block completition. <br/><br/>
/// 
/// For example, this block could let get groups of 10MBs when grouping smaller byte arrays together. <br/><br/>
/// 
/// The `chunkThreshold` parameter is value that you want per chunk (not to be exceeded, but could be less than). <br/><br/>
/// The `chunkSelector` parameter is the lambda you use to map an item to it's portion of the chunk value. <br/><br/>
/// 
/// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
/// </summary>
public class ChunkBlock<TItem> : IPropagatorBlock<TItem, TItem[]>, IReceivableSourceBlock<TItem[]>
{
    private readonly ITargetBlock<TItem> m_target;
    private readonly IReceivableSourceBlock<TItem[]> m_source;

    /// <summary>
    /// ChunkBlock is similar to the BatchBlock where TIn gets converted into TIn[] as the output. But, instead of just batching by a number of items, we batch but some summation threshold. 
    /// This is similar to the Chunk method in LINQ. When a chunk's sum exceeds the threshold, it will be emitted. An incomplete chunk will be emitted on block completition. <br/><br/>
    /// 
    /// For example, this block could let get groups of 10MBs when grouping smaller byte arrays together. <br/><br/>
    /// 
    /// The `chunkThreshold` parameter is value that you want per chunk (not to be exceeded, but could be less than). <br/><br/>
    /// The `chunkSelector` parameter is the lambda you use to map an item to it's portion of the chunk value. <br/><br/>
    /// 
    /// See <a href="https://github.com/hotfix-houdini/block-party">GitHub</a> for more details.
    /// </summary>
    public ChunkBlock(ulong chunkThreshold, Func<TItem, ulong> chunkSelector)
    {
        var chunk = new List<TItem>();
        var chunkValue = 0ul;

        var source = new BufferBlock<TItem[]>();
        var target = new ActionBlock<TItem>(item =>
        {
            var itemValue = chunkSelector(item);

            if (chunkValue + itemValue > chunkThreshold)
            {
                if (itemValue > chunkThreshold)
                {
                    throw new InvalidOperationException("Chunk Item has value greater than threshold.");
                }

                var posted = source.Post(chunk.ToArray());
                if (!posted)
                {
                    throw new FailedToPostException();
                }

                chunkValue = 0;
                chunk = [];
            }

            chunk.Add(item);
            chunkValue += itemValue;
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
                if (chunk.Any())
                {
                    var posted = source.Post(chunk.ToArray());
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
