using BlockParty.Builder;
using BlockParty.Tests.Blocks;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Builder;
public class DataflowBuilderTests
{
    [Theory]
    [TestCaseSource(nameof(DataPipelineShouldFlowTestCases))]
    public async Task DataPipelineShouldFlow<TInput, TOutput>(
        TInput[] input,
        IPropagatorBlock<TInput, TOutput> builtPipeline,
        TOutput[] expectedOutput)
    {
        // arrange
        var data = CompletedBufferBlockFromList(input);

        // act
        data.LinkTo(builtPipeline, new DataflowLinkOptions() { PropagateCompletion = true });
        var results = await ReadAllAsync(builtPipeline);

        // assert
        Assert.That(results, Is.EqualTo(expectedOutput).AsCollection);
    }

    [Test]
    public async Task Action_ShouldExecuteForEachItem_AndComplete()
    {
        // arrange
        var data = CompletedBufferBlockFromList(Array(1, 2, 3));

        var actualActedOnItems = new List<int>();
        var pipeline = new DataflowBuilder<int>()
            .Action(i => actualActedOnItems.Add(i))
            .Build();

        // act
        data.LinkTo(pipeline, new DataflowLinkOptions() { PropagateCompletion = true });
        await pipeline.Completion; // completed data buffer propagated already; no need to explicity complete pipeline

        // assert
        var expectedActedOnItems = new List<int>() { 1, 2, 3 };
        Assert.That(actualActedOnItems, Is.EqualTo(expectedActedOnItems).AsCollection);
    }

    [Test]
    public async Task SimpleExample()
    {
        // arrange
        var intermediatePipeline = new DataflowBuilder<int>()
            .Filter(n => n % 2 == 1)    // filters stream to odd numbers
            .Transform(n => $"{n + 1}") // maps odd numbers to the next even number as strings
            .Build();                   // generates an IPropagatorBlock for use

        // act
        for (int i = 1; i <= 4; i++)
        {
            intermediatePipeline.Post(i);
        }
        intermediatePipeline.Complete();

        var results = new List<string>();
        var downstreamBlock = new ActionBlock<string>(s => results.Add(s));
        intermediatePipeline.LinkTo(downstreamBlock, new DataflowLinkOptions() { PropagateCompletion = true });
        await downstreamBlock.Completion;

        // assert
        /* flow:
         *  in: 1, 2, 3, 4
         *  => 1, 3
         *  => "2", "4"
         */
        Assert.That(results, Is.EqualTo(new string[] { "2", "4" }).AsCollection);
    }

    [Test]
    public async Task SimpleActionExample()
    {
        // arrange
        var sum = 0.0;
        var endingPipeline = new DataflowBuilder<int[]>()
            .TransformMany(numbers => numbers) // flatten array
            .Filter(n => n % 2 == 0)           // filters stream to even numbers
            .Transform(n => n + 0.5)           // maps even numbers to the next odd number as strings
            .Action(n => sum += n)             // add the strings to an arra
            .Build();

        // act
        for (int i = 1; i <= 4; i++)
        {
            endingPipeline.Post([i, i + 1]);
        }
        endingPipeline.Complete();
        await endingPipeline.Completion; // no need (or option) to link to this pipeline downstream.

        // assert
        /* flow:
         *  in: [1,2], [2,3], [3,4], [4,5]
         *  => 1, 2, 2, 3, 3, 4, 4, 5
         *  => 2, 2, 4, 4
         *  => 2.5, 2.5, 4.5, 4.5
         *  => 14
         */
        Assert.That(sum, Is.EqualTo(14.0));
    }

    [Test]
    public async Task SimpleKafkaExample()
    {
        // arrange
        var results = new List<string>();
        var unbuiltPartitionedPipeline = new DataflowBuilder<int>()
            .Kafka(
                keySelector: i => i % 3,
                allowedKeys: [0, 1],                                   // can fitler out while partitioning; no n % 3 == 2 results 
                (key, builder) =>                                      // now you continue with a "recipe" builder that gets replicated per allowedKey
                    builder.Transform(i => $"{i} % 3 == {key}"))       // you have access to the key
            .Batch(2)                                                  // allow partitions fan back in
            .TransformMany(stringBatch => stringBatch.OrderBy(s => s)) // our in-order guarantee is only per-partition, not globally; lets sort for the deterministic test assertion 
            .Action(s => results.Add(s));
        var mermaidGraph = unbuiltPartitionedPipeline.GenerateMermaidGraph(); // can debug and access the mermaid graph
        var partitionedPipeline = unbuiltPartitionedPipeline.Build();

        // act
        partitionedPipeline.Post(0); // partition 0
        partitionedPipeline.Post(1); // partition 1
        partitionedPipeline.Post(2); // partition 2; filtered out
        partitionedPipeline.Post(3); // partition 0
        partitionedPipeline.Post(4); // partition 1
        partitionedPipeline.Post(5); // partition 2; filtered out
        partitionedPipeline.Complete();
        await partitionedPipeline.Completion;

        // assert
        Assert.That(results, Is.EqualTo([
            "0 % 3 == 0",
            "1 % 3 == 1",
            "3 % 3 == 0",
            "4 % 3 == 1"]).AsCollection);
    }

    [Test]
    public void GenerateMermaidGraph_ShouldCreateExpectedGraph()
    {
        // arrange
        var sum = 0.0;
        var unbuiltPipeline = new DataflowBuilder<int>()
            .Transform(i => new int[] { i, i + 1 })
            .TransformMany(numbers => numbers)
            .Filter(i => i % 2 == 0)
            .Beam<TestAccumulator>(
                    window: TimeSpan.FromSeconds(2),
                    (i, acc) => acc.Value += i,
                    (i, _) => i * 1_000_000_000)
            .Batch(2)
            .Kafka(
                keySelector: batch => batch.Count(),
                allowedKeys: [0, 1, 2],
                (key, builder) => builder.Transform(batch => batch.Count()))
            .Action(batchCounts => sum += batchCounts)
            .Action(async batchDone => await Task.Delay(1));

        // act
        var mermaidGraph = unbuiltPipeline.GenerateMermaidGraph();

        // assert
        Assert.That(mermaidGraph, Is.EqualTo(@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  transform_1[""TransformBlock&lt;Int32,Int32[]&gt;""]
  transformMany_2[""TransformManyBlock&lt;Int32[],Int32&gt;""]
  filter_3[""FilterBlock&lt;Int32&gt;""]
  beam_4[""BeamBlock&lt;Int32,TestAccumulator&gt;""]
  batch_5[""BatchBlock&lt;TestAccumulator&gt;""]
  action_6[""ActionBlock&lt;TestAccumulator[]&gt;""]
  buffer_7[""BufferBlock&lt;TestAccumulator[]&gt;""]
  buffer_9[""BufferBlock&lt;TestAccumulator[]&gt;""]
  buffer_11[""BufferBlock&lt;TestAccumulator[]&gt;""]
  transform_8[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  transform_10[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  transform_12[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  buffer_13[""BufferBlock&lt;Int32&gt;""]
  transform_14[""TransformBlock&lt;Int32,DoneResult&gt;""]
  transform_15[""TransformBlock&lt;DoneResult,DoneResult&gt;""]

  buffer_0 --> transform_1
  transform_1 --> transformMany_2
  transformMany_2 --> filter_3
  filter_3 --> beam_4
  beam_4 --> batch_5
  batch_5 --> action_6
  action_6 --> buffer_7
  action_6 --> buffer_9
  action_6 --> buffer_11
  buffer_7 --> transform_8
  buffer_9 --> transform_10
  buffer_11 --> transform_12
  transform_8 --> buffer_13
  transform_10 --> buffer_13
  transform_12 --> buffer_13
  buffer_13 --> transform_14
  transform_14 --> transform_15
```
"));
    }

    [Test]
    public async Task Kafka_ShouldWaitForAllReplicatedPipelinesToCompleteBeforePropagating()
    {
        // arrange
        var results = new List<string>();
        var onesDone = 0;
        var all1sDone = new TaskCompletionSource();
        var pipeline = new DataflowBuilder<int>()
            .Kafka(
                keySelector: i => i % 3,
                allowedKeys: [1, 2],
                replicatedPipeline: (key, builder) => builder
                    .Transform(i =>
                    {
                        if (key == 2)
                        {
                            all1sDone.Task.Wait();
                            Task.Delay(50).Wait();
                        }

                        return i;
                    })
                    .TransformMany(i => Enumerable.Range(0, i).Select(j => $"{key}"))
                    .Transform(i => (value: i, key: key)))
            .Transform(downstreamTuple =>
            {
                if (downstreamTuple.key == 1)
                {
                    onesDone++;
                    if (onesDone == 4)
                    {
                        all1sDone.SetResult();
                    }
                }
                return downstreamTuple.value;
            })
            .Action(x => results.Add(x))
            .Build();

        // act
        pipeline.Post(4);
        pipeline.Post(5);
        pipeline.Post(6);
        pipeline.Complete();
        await pipeline.Completion;

        // assert
        Assert.That(results, Is.EqualTo(["1", "1", "1", "1", "2", "2", "2", "2", "2"]).AsCollection);
    }

    [Test]
    public void Kafka_ShouldPropagateFaultsIfAllKafkaPipelinesFail()
    {
        // arrange
        var pipeline = new DataflowBuilder<int>()
            .Kafka(
                keySelector: i => i % 2,
                allowedKeys: [0, 1],
                replicatedPipeline: (key, builder) => builder
                    .Action(i => throw new Exception($"should bomb here {key}")))
            .Action(doneSignals => throw new Exception("might get here"))
            .Build();

        // act
        pipeline.Post(1);
        pipeline.Post(2);
        pipeline.Complete();
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await pipeline.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("should bomb here 0").Or.Contains("should bomb here 1")); // non-deterministic
    }

    [Test]
    public void Kafka_ShouldBreakOutOfInfiniteStreamIfAPartitionFaults()
    {
        // arrange
        var pipeline = new DataflowBuilder<int>()
            .Kafka(
                keySelector: i => i % 2,
                allowedKeys: [0, 1],
                replicatedPipeline: (key, builder) => builder
                    .Action(i =>
                    {
                        if (key == 0)
                        {
                            throw new Exception($"should bomb here {key}");
                        }
                    }))
            .Action(doneSignals => { })
            .Build();

        // act    
        pipeline.Post(0);
        pipeline.Post(1);
        // no complete
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await pipeline.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("should bomb here 0"));
    }

    [Test]
    public void Kafka_ShouldPropagateFaultsIfUpstreamBlocksFail() // Wraps the exception once? I'm okay with it
    {
        // arrange
        var pipeline = new DataflowBuilder<int>()
            .Transform(i =>
            {
                if (i == 7)
                {
                    throw new Exception("7 ate 9!!!!!");
                }
                return i;
            })
            .Kafka(
                keySelector: i => i % 2,
                allowedKeys: [0, 1],
                replicatedPipeline: (key, builder) => builder
                    .Action(async i =>
                    {
                        await Task.Delay(1);
                    }))
            .Action(doneSignals => { })
            .Build();

        // act
        for (int i = 0; i <= 9; i++)
        {
            pipeline.Post(i);
        }
        pipeline.Complete();
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await pipeline.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("7 ate 9!!!!!"));
    }

    [Test]
    public async Task Kafka_WithEndingAction_ShouldNotNullTargetTheSignalsAsync()
    {
        // arrange
        var doneSignalCount = 0;
        var pipeline = new DataflowBuilder<int>()
            .Kafka(
                keySelector: i => i % 2,
                allowedKeys: [0, 1],
                replicatedPipeline: (key, builder) => builder
                    .Action(async i =>
                    {
                        await Task.Delay(1);
                    }))
            .Action(doneSignal => doneSignalCount++)
            .Build();

        // act
        pipeline.Post(1);
        pipeline.Post(2);
        pipeline.Complete();
        await pipeline.Completion;

        // assert
        Assert.That(doneSignalCount, Is.EqualTo(2));
    }

    private static IEnumerable<TestCaseData> DataPipelineShouldFlowTestCases()
    {
        yield return new TestCaseData(
            Array(["1", "2", "3"]),
            new DataflowBuilder<string>()
                .Build(),
            Array(["1", "2", "3"])
        ).SetName("build first just propagates");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0)
                .Build(),
            Array([0, 2, 4])
        ).SetName("Filter should filter stream");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0)
                .Filter(x => x != 4)
                .Build(),
            Array([0, 2])
        ).SetName("builder should support multiple Filters");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Transform(x => x * 2)
                .Build(),
            Array([0, 2, 4])
        ).SetName("Transform should transform stream");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Transform(x => x * 2)
                .Transform(x => x + 1)
                .Build(),
            Array([1, 3, 5])
        ).SetName("builder should support multiple Transforms");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 1) // 1, 3, 5
                .Transform(x => x + 1)   // 2, 4, 6
                .Filter(x => x < 6)      // 2, 4
                .Transform(x => x - 2)   // 0, 2
                .Build(),
            Array([0, 2])
        ).SetName("Transform and Filters should be interchangeable");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Transform(x => $"{x}-str")
                .Build(),
            Array(["0-str", "1-str", "2-str"])
        ).SetName("Transform should support transforming to different types");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Transform(x => $"{x}-str")    // int -> str
                .Transform(x => new { A = x }) // str -> anon object
                .Build(),
            Array([new { A = "0-str" }, new { A = "1-str" }, new { A = "2-str" }])
        ).SetName("Transform should support multiple type transforms");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0) // 0, 2, 4
                .Transform(x => $"{x}")  // "0", "2", "4"
                .Build(),
            Array(["0", "2", "4"])
        ).SetName("should allow Filter before Transform to different type");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0)       // 0, 2, 4
                .Transform(x => $"{x}")        // "0", "2", "4"
                .Filter(x => int.Parse(x) > 0) // "2", "4"
                .Build(),
            Array(["2", "4"])
        ).SetName("should allow Filter after Transform to different type");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0)                   // 0, 2, 4
                .Transform(x => $"{x}")                    // "0", "2", "4"
                .Filter(x => int.Parse(x) > 0)             // "2", "4"
                .Transform(x => int.Parse(x) * 2)          // 4, 8
                .Filter(x => x > 5)                        // 8
                .Transform(x => x == 8 ? "ate" : "hungry") // "ate"
                .Build(),
            Array(["ate"])
        ).SetName("multiple Transform, Filters, and transforms, should all work");

        yield return new TestCaseData(
            Array(
                Array(0, 1, 2),
                Array(3, 4, 5)
                ),
            new DataflowBuilder<int[]>()
                .TransformMany(xes => xes)
                .Build(),
            Array(0, 1, 2, 3, 4, 5)
        ).SetName("Should support Transform manys");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4, 5),
            new DataflowBuilder<int>()
                .Filter(x => x % 2 == 0)          // 0, 2, 4
                .Transform(x => new[] {x, x + 1}) // [0, 1], [2, 3], [4, 5]
                .TransformMany(xes => xes)        // 0, 1, 2, 3, 4, 5
                .Transform(x => $"{x}_{x}")       // "0_0", "1_1", "2_2", "3_3", "4_4", "5_5"
                .Transform(x => x.Split("_"))     // ["0", "0"], ["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"], ["5", "5"]
                .TransformMany(xes => xes)        // "0", "0", "1", "1", "2", "2", "3", "3", "4", "4", "5", "5"
                .Filter(x => int.Parse(x) > 0)    // "1", "1", "2", "2", "3", "3", "4", "4", "5", "5"
                .Transform(x => int.Parse(x) * 2) // 2, 2, 4, 4, 6, 6, 8, 8, 10, 10
                .Filter(x => x > 5)               // 6, 6, 8, 8, 10, 10
                .Build(),
            Array(6, 6, 8, 8, 10, 10)
        ).SetName("complicated Transform manys");

        // just checking the runtime of the test
        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Action(async doneResult => await Task.Delay(100))
                .Build(),
            new DoneResult[] { } // gets consumed by null pointer (unless linked with a prepend!)
        ).SetName("should be able to do async/await for each");

        yield return new TestCaseData(
            Array(0, 1, 2, 3),
            new DataflowBuilder<int>()
                .Transform(i => (time: i, value: i * 2))
                .Beam<TestAccumulator>(
                    window: TimeSpan.FromSeconds(2),
                    (stream, acc) => acc.Value += stream.value,
                    (stream, _) => stream.time * 1_000_000_000)
                .Transform(acc => (start: acc.WindowStart, end: acc.WindowEnd, value: acc.Value))
                .Build(),
            Array<(long, long, int)>(
                new Window(start: 0_000000000, end: 2_000000000, value: 2 * (0 + 1)),
                new Window(start: 2_000000000, end: 4_000000000, value: 2 * (2 + 3)))
        ).SetName("should be able to beam");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4),
            new DataflowBuilder<int>()
                .Batch(2)
                .Build(),
            Array<int[]>([0, 1], [2, 3], [4])
        ).SetName("batch should work with divisible amount");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4, 5),
            new DataflowBuilder<int>()
                .Batch(2)
                .Build(),
            Array<int[]>([0, 1], [2, 3], [4, 5])
        ).SetName("batch should work with remainder amount");

        yield return new TestCaseData(
            Array(0),
            new DataflowBuilder<int>()
                .Batch(2)
                .Build(),
            Array<int[]>([0])
        ).SetName("batch should work with less than batch amount streamed");

        yield return new TestCaseData(
            Array(4, 5, 6),
            new DataflowBuilder<int>()
                .Kafka(
                    keySelector: i => i % 3,                                                // partition into 0, 1, or 2
                    allowedKeys: [1, 2],                                                    // only replicate pipeline for key == 1 or 2.
                    replicatedPipeline: (key, builder) => builder                           // continue the chain with access to the partitionKey
                        .TransformMany(i => Enumerable.Range(0, i).Select(j => $"{key}")))  // 4 => 4 "1"'s , 5 => 5 "2"'s, 6 => filtered out. Demonstrates partition key usage.
                .Batch(9)                                                                   // combine all results so we can deterministically order them
                .TransformMany(combinedResults => combinedResults.OrderBy(s => s))          // deterministically order results for assertion
                .Build(),
            Array("1", "1", "1", "1", "2", "2", "2", "2", "2")
        ).SetName("kafka should fanout and recombine");

        // kafka in kafka should flow
    }

    private static T[] Array<T>(params T[] elements) => elements;

    private static BufferBlock<T> CompletedBufferBlockFromList<T>(IEnumerable<T> collection)
    {
        var block = new BufferBlock<T>();
        foreach (var item in collection)
        {
            block.Post(item);
        }
        block.Complete();

        return block;
    }

    private static async Task<List<TOutput>> ReadAllAsync<TInput, TOutput>(IPropagatorBlock<TInput, TOutput> block)
    {
        var bufferBlock = new BufferBlock<TOutput>();

        block.LinkTo(bufferBlock, new DataflowLinkOptions { PropagateCompletion = true });

        var results = new List<TOutput>();
        await foreach (var item in bufferBlock.ReceiveAllAsync())
        {
            results.Add(item);
        }

        await block.Completion;
        return results;
    }
}

// just to avoid a silly struct-compiler warning
internal record struct Window(long start, long end, int value)
{
    public static implicit operator (long start, long end, int value)(Window value)
    {
        return (value.start, value.end, value.value);
    }

    public static implicit operator Window((long start, long end, int value) value)
    {
        return new Window(value.start, value.end, value.value);
    }
}