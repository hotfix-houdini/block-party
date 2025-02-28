using BlockParty.Blocks.Beam;
using BlockParty.Blocks.Filter;
using BlockParty.Visualizer;
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
    protected List<BlockNode> _blockChain;

    private static readonly DataflowLinkOptions _linkOptions = new()
    {
        PropagateCompletion = true
    };

    protected DataflowBuilder(BufferBlock<TInput> inputBlock)
        : this(inputBlock, null, [ BlockNode.Create<TInput, TInput>("BufferBlock") ])
    {
    }

    private DataflowBuilder(BufferBlock<TInput> originalBlock, ISourceBlock<TOutput> currentBlock, List<BlockNode> blockChain)
    {
        _sourceBlock = originalBlock;
        _lastBlock = currentBlock;
        _blockChain = blockChain;
    }

    public DataflowBuilder<TInput, TNewType> Transform<TNewType>(Func<TOutput, TNewType> lambda)
    {
        var newBlock = new TransformBlock<TOutput, TNewType>(lambda);
        AddBlock(newBlock, "TransformBlock");
        return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock, _blockChain);
    }

    public DataflowBuilder<TInput, TNewType> TransformMany<TNewType>(Func<TOutput, IEnumerable<TNewType>> lambda)
    {
        var newBlock = new TransformManyBlock<TOutput, TNewType>(lambda);
        AddBlock(newBlock, "TransformManyBlock");
        return new DataflowBuilder<TInput, TNewType>(_sourceBlock, newBlock, _blockChain);
    }

    public DataflowBuilder<TInput, TOutput> Filter(FilterBlock<TOutput>.Predicate predicate)
    {
        var newBlock = new FilterBlock<TOutput>(predicate);
        AddBlock(newBlock, "FilterBlock");
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
        AddBlock(newBlock, "TransformBlock");
        return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock, _blockChain);
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
        AddBlock(newBlock, "TransformBlock");
        return new DataflowBuilder<TInput, DoneResult>(_sourceBlock, newBlock, _blockChain);
    }

    public DataflowBuilder<TInput, TAccumulator> Beam<TAccumulator>(
        TimeSpan window,
        Action<TOutput, TAccumulator> accumlateMethod,
        Func<TOutput, NanosecondTimeConverter, long> timeSelectionMethod,
        BeamBlockSettings settings = null) where TAccumulator : class, IAccumulator, new()
    {
        var newBlock = new BeamBlock<TOutput, TAccumulator>(window, accumlateMethod, timeSelectionMethod, settings);
        AddBlock(newBlock, "BeamBlock");
        return new DataflowBuilder<TInput, TAccumulator>(_sourceBlock, newBlock, _blockChain);
    }

    public DataflowBuilder<TInput, TOutput[]> Batch(int batchSize)
    {
        var newBlock = new BatchBlock<TOutput>(batchSize);
        AddBlock(newBlock, "BatchBlock");
        return new DataflowBuilder<TInput, TOutput[]>(_sourceBlock, newBlock, _blockChain);
    }

    public DataflowBuilder<TInput, TReplicatedPipelineOutput> Kafka<TKey, TReplicatedPipelineOutput>(
        Func<TOutput, TKey> keySelector,
        IEnumerable<TKey> allowedKeys,
        Func<TKey, DataflowBuilder<TOutput>, DataflowBuilder<TOutput, TReplicatedPipelineOutput>> replicatedPipeline)
        where TKey : IEquatable<TKey>
    {
        var upstreamSignaler = new TaskCompletionSource<Exception>();

        var recombinedBlock = new BufferBlock<TReplicatedPipelineOutput>();
        var partitionPipelines = allowedKeys.ToDictionary(
            keySelector: key => key,
            elementSelector: key =>
            {
                var independentBuilder = new DataflowBuilder<TOutput>();
                var independentPipeline = replicatedPipeline(key, independentBuilder).BuildWithoutActionNullTargeting();
                independentPipeline.LinkTo(recombinedBlock); // NOT with auto-complete propagation (which is greedy; doesn't wait for all pipelines) 

                // propagate upstream complete/fault
                _ = upstreamSignaler.Task.ContinueWith(resultTask =>
                {
                    if (resultTask.Result != null)
                    {
                        independentPipeline.Fault(resultTask.Result);
                    }
                    else
                    {
                        independentPipeline.Complete();
                    }
                });

                // propagate faults to other partitions
                _ = independentPipeline.Completion.ContinueWith(pipelineCompletion =>
                {
                    if (pipelineCompletion.IsFaulted)
                    {
                        upstreamSignaler.TrySetResult(pipelineCompletion.Exception);

                        // fault downstream fast too
                        ((IDataflowBlock)recombinedBlock).Fault(new AggregateException(pipelineCompletion.Exception));
                    }
                });

                return independentPipeline;
            });
        var dispatcherBlock = new ActionBlock<TOutput>(async item =>
        {
            var partitionKeyMatch = partitionPipelines.TryGetValue(keySelector(item), out var partitionPipeline);
            if (partitionKeyMatch)
            {
                await partitionPipeline.SendAsync(item);
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
                    ((IDataflowBlock)recombinedBlock).Fault(new AggregateException(flattenedExceptions));
                }
                else
                {
                    recombinedBlock.Complete();
                }
            });

        // add block chains
        _lastBlock.LinkTo(dispatcherBlock, _linkOptions);
        return new DataflowBuilder<TInput, TReplicatedPipelineOutput>(_sourceBlock, recombinedBlock, _blockChain);
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
        var visualizer = new PipelineVisualizer();
        return visualizer.Visualize(_blockChain);
    }

    private void AddBlock<TNewType>(IPropagatorBlock<TOutput, TNewType> newBlock, string name)
    {
        _lastBlock.LinkTo(newBlock, _linkOptions);
        _lastBlock = newBlock as ISourceBlock<TOutput>;
        _blockChain.Add(BlockNode.Create<TOutput, TNewType>(name));
    }
}