using BlockParty.Blocks.BatchBy;
using BlockParty.Blocks.Beam;
using BlockParty.Blocks.Filter;
using BlockParty.Builder.DAG;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder;

public class DataflowBuilder<TInput, TOutput>
{
    protected readonly BufferBlock<TInput> _sourceBlock;
    protected ISourceBlock<TOutput> _lastBlock;
    protected DataflowDAG<TInput, TOutput> _blockDag;
    private static readonly DataflowLinkOptions _linkOptions = new()
    {
        PropagateCompletion = true
    };

    protected DataflowBuilder(BufferBlock<TInput> inputBlock)
        : this(inputBlock, null, new DataflowDAG<TInput, TOutput>())
    {
    }

    protected DataflowBuilder(BufferBlock<TInput> inputBlock, DataflowDAG<TInput, TOutput> startingDag)
        : this(inputBlock, null, startingDag)
    {
    }

    private DataflowBuilder(BufferBlock<TInput> originalBlock, ISourceBlock<TOutput> currentBlock, DataflowDAG<TInput, TOutput> blockDag)
    {
        _sourceBlock = originalBlock;
        _lastBlock = currentBlock;
        _blockDag = blockDag;
    }

    public DataflowBuilder<TInput, TNewType> Transform<TNewType>(Func<TOutput, TNewType> lambda)
    {
        var newBlock = new TransformBlock<TOutput, TNewType>(lambda);
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TNewType> TransformMany<TNewType>(Func<TOutput, IEnumerable<TNewType>> lambda)
    {
        var newBlock = new TransformManyBlock<TOutput, TNewType>(lambda);
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TOutput> Filter(FilterBlock<TOutput>.Predicate predicate)
    {
        var newBlock = new FilterBlock<TOutput>(predicate);
        _blockDag = AddBlock(newBlock);
        return this;
    }

    /// <summary>
    /// Note: This actually creates a TransformBlock instead of an Action block, where the output is a "DoneResult" object.
    /// The intention is not to link downstream of this; it was done this way to maintain the IPropagator interface.
    /// Additionally, if the last block in the pipeline outputs to DoneResult, then it will automatically be linked to the NullTarget block so it will process messages and complete automatically.
    /// If for SOME reason you build a pipeline and end it with Action, AND want to link downstream of it (just receiving DoneResults), then you want to link to with the PREPEND instead of APPEND option.
    /// </summary>
    public DataflowBuilder<TInput, DoneResult> Action(Action<TOutput> lambda)
    {
        var newBlock = new TransformBlock<TOutput, DoneResult>(input =>
        {
            lambda(input);
            return DoneResult.Instance;
        });
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock, newDag);
    }

    /// <summary>
    /// Note: This actually creates a TransformBlock instead of an Action block, where the output is a "DoneResult" object.
    /// The intention is not to link downstream of this; it was done this way to maintain the IPropagator interface.
    /// Additionally, if the last block in the pipeline outputs to DoneResult, then it will automatically be linked to the NullTarget block so it will process messages and complete automatically.
    /// If for SOME reason you build a pipeline and end it with Action, AND want to link downstream of it (just receiving DoneResults), then you want to link to with the PREPEND instead of APPEND option.
    /// </summary>
    public DataflowBuilder<TInput, DoneResult> Action(Func<TOutput, Task> lambda)
    {
        var newBlock = new TransformBlock<TOutput, DoneResult>(async input =>
        {
            await lambda(input);
            return DoneResult.Instance;
        });
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TAccumulator> Beam<TAccumulator>(
        TimeSpan window,
        Action<TOutput, TAccumulator> accumlateMethod,
        Func<TOutput, NanosecondTimeConverter, long> timeSelectionMethod,
        BeamBlockSettings settings = null) where TAccumulator : class, IAccumulator, new()
    {
        var newBlock = new BeamBlock<TOutput, TAccumulator>(window, accumlateMethod, timeSelectionMethod, settings);
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TAccumulator>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TOutput[]> Batch(int batchSize)
    {
        var newBlock = new BatchBlock<TOutput>(batchSize);
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TOutput[]>(_sourceBlock, newBlock, newDag);
    }

    /// <summary>
    /// This introduces controlled parallelism into a pipeline where we can maintain in-order guarantees while horizontally scaling (in process) by an independent key.
    /// This also fans back in so the in-order processing can continue, or so all branches can be awaited for completion.
    /// <br/><br/>
    /// 
    /// This overload maps elements to a single partition without message duplication.
    /// </summary>
    public DataflowBuilder<TInput, TReplicatedPipelineOutput> Kafka<TPartition, TReplicatedPipelineOutput>(
        Func<TOutput, TPartition> singlePartitionSelector,
        IEnumerable<TPartition> partitions,
        Func<TPartition, DataflowBuilder<TOutput>, DataflowBuilder<TOutput, TReplicatedPipelineOutput>> replicatedPipeline)
        where TPartition : IEquatable<TPartition>
    {
        IEnumerable<TPartition> wrappedEnumerableSelector(TOutput item) => [singlePartitionSelector(item)];
        return Kafka(wrappedEnumerableSelector, partitions, replicatedPipeline);
    }

    /// <summary>
    /// This introduces controlled parallelism into a pipeline where we can maintain in-order guarantees while horizontally scaling (in process) by an independent key.
    /// This also fans back in so the in-order processing can continue, or so all branches can be awaited for completion.
    /// <br/><br/>
    /// 
    /// This overload allows replicating messages to multiple partitions.
    /// </summary>
    public DataflowBuilder<TInput, TReplicatedPipelineOutput> Kafka<TPartition, TReplicatedPipelineOutput>(
        Func<TOutput, IEnumerable<TPartition>> multiPartitionSelector,
        IEnumerable<TPartition> partitions,
        Func<TPartition, DataflowBuilder<TOutput>, DataflowBuilder<TOutput, TReplicatedPipelineOutput>> replicatedPipeline)
        where TPartition : IEquatable<TPartition>
    {
        if (!partitions.Any())
        {
            throw new ArgumentException("1 or more partitions is required.");
        }

        var upstreamSignaler = new TaskCompletionSource<Exception>();

        var recombinedBlock = new BufferBlock<TReplicatedPipelineOutput>();
        var nodesPerPartition = replicatedPipeline(partitions.First(), new DataflowBuilder<TOutput>())._blockDag.NodeCount;
        var fannedOutDags = new List<DataflowDAG<TOutput, TReplicatedPipelineOutput>>();
        var partitionPipelines = partitions.Select((key, i) => (key, i)).ToDictionary(
            keySelector: indexedKey => indexedKey.key,
            elementSelector: indexedKey =>
            {
                var offsetDag = new DataflowDAG<TOutput, TOutput>(_blockDag.NextNodeId + 1 + (indexedKey.i * nodesPerPartition));
                var unbuiltPartition = replicatedPipeline(indexedKey.key, new DataflowBuilder<TOutput>(offsetDag));
                fannedOutDags.Add(unbuiltPartition._blockDag);

                var singlePartition = unbuiltPartition.BuildWithoutActionNullTargeting();
                singlePartition.LinkTo(recombinedBlock); // NOT with auto-complete propagation (which is greedy; doesn't wait for all pipelines) 

                // propagate upstream complete/fault
                _ = upstreamSignaler.Task.ContinueWith(resultTask =>
                {
                    if (resultTask.Result != null)
                    {
                        singlePartition.Fault(resultTask.Result);
                    }
                    else
                    {
                        singlePartition.Complete();
                    }
                });

                // propagate faults sideways
                _ = singlePartition.Completion.ContinueWith(pipelineCompletion =>
                {
                    if (pipelineCompletion.IsFaulted)
                    {
                        upstreamSignaler.TrySetResult(pipelineCompletion.Exception);

                        // fault downstream fast too
                        ((IDataflowBlock)recombinedBlock).Fault(new AggregateException(pipelineCompletion.Exception));
                    }
                });

                return singlePartition;
            });
        // ActionBlock lookup for O(1) fanouts vs .LinkTo(..)'s looping behavior.
        var dispatcherBlock = new ActionBlock<TOutput>(async item =>
        {
            var partitionsToDispatchTo = multiPartitionSelector(item);
            foreach (var partition in partitionsToDispatchTo)
            {
                var partitionKeyMatch = partitionPipelines.TryGetValue(partition, out var partitionPipeline);
                if (partitionKeyMatch)
                {
                    await partitionPipeline.SendAsync(item);
                }
            }

            // implicitly filter out un-allowed partition keys
        });
        dispatcherBlock.Completion.ContinueWith(upstreamSignal => upstreamSignaler.TrySetResult(upstreamSignal.Exception));

        // chain partition completion downstream
        _ = Task
            .WhenAll(partitionPipelines.Values.Select(p => p.Completion))
            .ContinueWith(allPartitionsDone =>
            {
                if (allPartitionsDone.IsFaulted)
                {
                    var flattenedExceptions = allPartitionsDone.Exception.Flatten().InnerExceptions.Distinct();
                    ((IDataflowBlock)recombinedBlock).Fault(new AggregateException(flattenedExceptions)); // almost always a noop with the fast-fault?
                }
                else
                {
                    recombinedBlock.Complete();
                }
            });

        _lastBlock.LinkTo(dispatcherBlock, _linkOptions);
        var newDag = _blockDag.AddFanOutAndIn(
            dispatcherBlock,
            fannedOutDags,
            recombinedBlock);
        return new DataflowBuilder<TInput, TReplicatedPipelineOutput>(_sourceBlock, recombinedBlock, newDag);
    }

    public DataflowBuilder<TInput, TOutput> Tap(Action<TOutput> lambda)
    {
        var newBlock = new TransformBlock<TOutput, TOutput>(input =>
        {
            lambda(input);
            return input;
        });
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TOutput>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TOutput> Tap(Func<TOutput, Task> lambda)
    {
        var newBlock = new TransformBlock<TOutput, TOutput>(async input =>
        {
            await lambda(input);
            return input;
        });
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TOutput>(_sourceBlock, newBlock, newDag);
    }

    public DataflowBuilder<TInput, TOutput[]> BatchBy<UGroup>(Func<TOutput, UGroup> selector)
        where UGroup : IEquatable<UGroup>
    {
        var newBlock = new BatchByBlock<TOutput, UGroup>(selector);
        var newDag = AddBlock(newBlock);
        return new DataflowBuilder<TInput, TOutput[]>(_sourceBlock, newBlock, newDag);
    }

    public IPropagatorBlock<TInput, TOutput> Build()
    {
        if (typeof(TOutput) == typeof(DoneResult))
        {
            _lastBlock.LinkTo(DataflowBlock.NullTarget<TOutput>());
        }
        return BuildWithoutActionNullTargeting();
    }

    protected IPropagatorBlock<TInput, TOutput> BuildWithoutActionNullTargeting()
    {
        return DataflowBlock.Encapsulate(_sourceBlock, _lastBlock);
    }

    public string GenerateMermaidGraph()
    {
        return _blockDag.GenerateMermaidGraph();
    }

    private DataflowDAG<TInput, TNewType> AddBlock<TNewType>(IPropagatorBlock<TOutput, TNewType> newBlock)
    {
        _lastBlock.LinkTo(newBlock, _linkOptions);
        _lastBlock = newBlock as ISourceBlock<TOutput>;
        return _blockDag.Add(newBlock);
    }
}