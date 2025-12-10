using BlockParty.Builder;
using BlockParty.Tests.Blocks;
using System.Diagnostics;
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
                singlePartitionSelector: i => i % 3,
                partitions: [0, 1],                                    // can fitler out while partitioning; no n % 3 == 2 results 
                (key, builder) =>                                      // now you continue with a "recipe" builder that gets replicated per allowedKey
                    builder.Transform(i => $"{i} % 3 == {key}"))       // you have access to the key
            .Batch(4)                                                  // fan partitions back in
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
    public async Task SimpleKafkaMessageDuplicationExample()
    {
        // arrange
        // pre-compute partition mapper for high performance
        var partitionMapper = new Dictionary<(bool hasDigit, bool hasLetter), string[]>()
        {
            {(hasDigit: false, hasLetter: false), [] },
            {(hasDigit: true, hasLetter: false), ["numbers"] },
            {(hasDigit: true, hasLetter: true), ["letters", "numbers"] },
            {(hasDigit: false, hasLetter: true), ["letters"] },
        };

        var results = new List<string>();
        var pubsubPipeline = new DataflowBuilder<string>()
            .Kafka(
                partitions: ["numbers", "letters"],
                multiPartitionSelector: s => partitionMapper[(s.Any(char.IsDigit), s.Any(char.IsLetter))], // lookup the partition given the strings digit/character affinity
                replicatedPipeline: (key, builder) => builder.Transform(s => $"{key}: {s}"))
            .Batch(500)
            .TransformMany(stringBatch => stringBatch.OrderBy(s => s))
            .Action(s => results.Add(s))
            .Build();

        // act
        pubsubPipeline.Post("abc");     // letters
        pubsubPipeline.Post("123");     // numbers
        pubsubPipeline.Post("abc 123"); // numbers and letters
        pubsubPipeline.Complete();
        await pubsubPipeline.Completion;

        // assert
        Assert.That(results, Is.EqualTo(
            [
                "letters: abc",
                "letters: abc 123",
                "numbers: 123",
                "numbers: abc 123" // "abc 123" message got duplicated because it was sent to 2 partitions
            ]).AsCollection);
    }

    [Test]
    public void GenerateMermaidGraph_ShouldCreateExpectedGraph()
    {
        // arrange
        var sum = 0.0;
        var windowCount = 0;
        var unbuiltPipeline = new DataflowBuilder<int>()
            .Transform(i => new int[] { i, i + 1 })
            .TransformMany(numbers => numbers)
            .Filter(i => i % 2 == 0)
            .Beam<TestAccumulator>(
                    window: TimeSpan.FromSeconds(2),
                    (i, acc) => acc.Value += i,
                    (i, _) => i * 1_000_000_000)
            .Tap(window => windowCount++)
            .Batch(2)
            .Kafka(
                singlePartitionSelector: batch => batch.Count(),
                partitions: [0, 1, 2],
                (key, builder) => builder.Transform(batch => batch.Count()))
            .BatchBy(batchCounts => batchCounts / 3)
            .Action(batchedBatchCounts => sum += batchedBatchCounts.Sum())
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
  transform_5[""TransformBlock&lt;TestAccumulator,TestAccumulator&gt;""]
  batch_6[""BatchBlock&lt;TestAccumulator&gt;""]
  action_7[""ActionBlock&lt;TestAccumulator[]&gt;""]
  buffer_8[""BufferBlock&lt;TestAccumulator[]&gt;""]
  buffer_10[""BufferBlock&lt;TestAccumulator[]&gt;""]
  buffer_12[""BufferBlock&lt;TestAccumulator[]&gt;""]
  transform_9[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  transform_11[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  transform_13[""TransformBlock&lt;TestAccumulator[],Int32&gt;""]
  buffer_14[""BufferBlock&lt;Int32&gt;""]
  batchBy_15[""BatchByBlock&lt;Int32,Int32&gt;""]
  transform_16[""TransformBlock&lt;Int32[],DoneResult&gt;""]
  transform_17[""TransformBlock&lt;DoneResult,DoneResult&gt;""]

  buffer_0 --> transform_1
  transform_1 --> transformMany_2
  transformMany_2 --> filter_3
  filter_3 --> beam_4
  beam_4 --> transform_5
  transform_5 --> batch_6
  batch_6 --> action_7
  action_7 --> buffer_8
  action_7 --> buffer_10
  action_7 --> buffer_12
  buffer_8 --> transform_9
  buffer_10 --> transform_11
  buffer_12 --> transform_13
  transform_9 --> buffer_14
  transform_11 --> buffer_14
  transform_13 --> buffer_14
  buffer_14 --> batchBy_15
  batchBy_15 --> transform_16
  transform_16 --> transform_17
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
                singlePartitionSelector: i => i % 3,
                partitions: [1, 2],
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
                singlePartitionSelector: i => i % 2,
                partitions: [0, 1],
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
                singlePartitionSelector: i => i % 2,
                partitions: [0, 1],
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
                singlePartitionSelector: i => i % 2,
                partitions: [0, 1],
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
                singlePartitionSelector: i => i % 2,
                partitions: [0, 1],
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

    [Test]
    public async Task Tap_ShouldAllowForSideEffects()
    {
        // arrange
        var tappedInts = new List<int>();
        var tappedStrings = new List<string>();
        var outputs = new List<string>();

        var pipeline = new DataflowBuilder<int>()
            .Tap(i => tappedInts.Add(i + 1))
            .Transform(i => $"{i + 2}")
            .Tap(s => tappedStrings.Add($"tap: {s}"))
            .Action(async value => 
            {
                await Task.Delay(1);
                outputs.Add(value);
            })
            .Build();

        // act
        pipeline.Post(1);
        pipeline.Post(2);
        pipeline.Post(3);
        pipeline.Complete();
        await pipeline.Completion;

        // assert
        Assert.That(tappedInts, Is.EqualTo([2, 3, 4]).AsCollection);
        Assert.That(tappedStrings, Is.EqualTo(["tap: 3", "tap: 4", "tap: 5"]).AsCollection);
        Assert.That(outputs, Is.EqualTo(["3", "4", "5"]).AsCollection);
    }

    [Test]
    public async Task ShouldBeAbleToConstructAnOrderedParallelPipeline()
    {
        // arrange
        var numbers = new List<int>();
        var pipeline = new DataflowBuilder<int>()
            .TransformMany(async i =>
            {
                if (i % 3 == 0) return []; // testing a filter while preserving paralleled-order
                await Task.Delay(100);
                return (IEnumerable<int>)[i];
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 3, EnsureOrdered = true })
            .Action(i => numbers.Add(i))
            .Build();

        // act
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        for(int i = 0; i < 15; i++)
        {
            pipeline.Post(i);
        }
        pipeline.Complete();
        await pipeline.Completion;
        stopwatch.Stop();

        // assert
        Assert.That(numbers, Has.Count.EqualTo(10));
        Assert.That(numbers, Is.EqualTo([1, 2, 4, 5, 7, 8, 10, 11, 13, 14]).AsCollection);
        Assert.That(stopwatch.Elapsed.TotalMilliseconds, Is.LessThanOrEqualTo(500)); // if ran synchronously, should be at least 1 second
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
                    singlePartitionSelector: i => i % 3,                                    // partition into 0, 1, or 2
                    partitions: [1, 2],                                                     // only replicate pipeline for key == 1 or 2.
                    replicatedPipeline: (key, builder) => builder                           // continue the chain with access to the partitionKey
                        .TransformMany(i => Enumerable.Range(0, i).Select(j => $"{key}")))  // 4 => 4 "1"'s , 5 => 5 "2"'s, 6 => filtered out. Demonstrates partition key usage.
                .Batch(9)                                                                   // combine all results so we can deterministically order them
                .TransformMany(combinedResults => combinedResults.OrderBy(s => s))          // deterministically order results for assertion
                .Build(),
            Array("1", "1", "1", "1", "2", "2", "2", "2", "2")
        ).SetName("kafka should fanout and recombine");

        yield return new TestCaseData(
            Array(4, 5, 6),
            new DataflowBuilder<int>()
                .Kafka(
                    singlePartitionSelector: i => i % 3,
                    partitions: [1, 2],
                    replicatedPipeline: (key, builder) => builder
                        .TransformMany(i => Enumerable.Range(0, i).Select(j => $"{i}")))
                        .Kafka(
                            singlePartitionSelector: i => int.Parse(i) % 2,
                            partitions: [1],
                            (innerKey, innerBuilder) => innerBuilder.Transform(s => $"[{innerKey}]: {s}"))
                .Batch(9)
                .TransformMany(combinedResults => combinedResults.OrderBy(s => s))
                .Build(),
            Array("[1]: 5", "[1]: 5", "[1]: 5", "[1]: 5", "[1]: 5")
        ).SetName("kafka in kafka should work");

        yield return new TestCaseData(
            Array("abc", "123", "abc 123"),
            new DataflowBuilder<string>()
                .Kafka(
                    partitions: ["numbers", "letters"],
                    multiPartitionSelector: s =>
                    {
                        var partitions = new List<string>();
                        var hasNumbers = s.Any(char.IsDigit);
                        var hasLetters = s.Any(char.IsLetter);
                        if (hasNumbers)
                        {
                            partitions.Add("numbers");
                        }
                        if (hasLetters)
                        {
                            partitions.Add("letters");
                        }

                        return partitions;
                    },
                    replicatedPipeline: (key, builder) => builder
                        .Transform(s => $"{key}: {s}"))
                .Batch(5000)
                .TransformMany(combinedResults => combinedResults.OrderBy(s => s))
                .Build(),
            Array(
                "letters: abc",
                "letters: abc 123",
                "numbers: 123",
                "numbers: abc 123" // message got duplicated to both letters and numbers partitions
                )
        ).SetName("kafka should support message duplication to multiple partitions");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4),
            new DataflowBuilder<int>()
                .BatchBy(i => i / 3)
                .Build(),
            Array<int[]>([0, 1, 2], [3, 4])
        ).SetName("batchBy should group contiguous groups");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4),
            new DataflowBuilder<int>()
                .Transform(async i => await Task.FromResult(42))
                .Build(),
            Array([42, 42, 42, 42, 42])
        ).SetName("transform should support async/await");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4),
            new DataflowBuilder<int>()
                .TransformMany(async i => await Task.FromResult((IEnumerable<int>)[42, 43]))
                .Build(),
            Array([42, 43, 42, 43, 42, 43, 42, 43, 42, 43])
        ).SetName("transformMany should support async/await");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4),
            new DataflowBuilder<int>()
                .Transform(i => i + 1, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 3, EnsureOrdered = true }) // 1 .. 5
                .Transform(async i => await Task.FromResult(42), new ExecutionDataflowBlockOptions() { EnsureOrdered = true }) // 42 .. 42 
                .TransformMany(i => new int[] { i, i + 1, i + 2, i + 3 }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 3, EnsureOrdered = true })
                .TransformMany(async i =>
                {
                    await Task.Delay(i);
                    return (IEnumerable<int>) [i, i + 1];
                }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 10, EnsureOrdered = true })
                .Tap(i => Console.WriteLine(i), new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 2, EnsureOrdered = true })
                .Tap(async i => await Task.FromResult(() => Console.WriteLine(i)), new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 2, EnsureOrdered = false })
                .Batch(8, new GroupingDataflowBlockOptions() { EnsureOrdered = true })
                .Action(group => { }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 3 })
                .Action(async group => await Task.FromResult(() => { }), new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 3 })
                .Build(),
            Array(new DoneResult[] { })
        ).SetName("block options should .... compile");
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