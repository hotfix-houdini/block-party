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
        CollectionAssert.AreEqual(expectedOutput, results);
    }

    [Test]
    public async Task ForEach_ShouldExecuteForEachItem()
    {
        // arrange
        var data = CompletedBufferBlockFromList(Array(1, 2, 3));

        var actualActedOnItems = new List<int>();
        var pipeline = new DataflowBuilder<int>()
            .ForEachAndComplete(i => actualActedOnItems.Add(i));

        // act
        data.LinkTo(pipeline, new DataflowLinkOptions() { PropagateCompletion = true });
        await pipeline.Completion; // completed data buffer propagated already; no need to explicity complete pipeline

        // assert
        var expectedActedOnItems = new List<int>() { 1, 2, 3 };
        CollectionAssert.AreEqual(expectedActedOnItems, actualActedOnItems);
    }

    [Test]
    public async Task SimpleExample()
    {
        // arrange
        var intermediatePipeline = new DataflowBuilder<int>()
            .Where(n => n % 2 == 1)  // filters stream to odd numbers
            .Select(n => $"{n + 1}") // maps odd numbers to the next even number as strings
            .Build();                // generates an IPropagatorBlock for use

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
    public async Task SimpleForeachExample()
    {
        // arrange
        var sum = 0.0;
        var endingPipeline = new DataflowBuilder<int[]>()
            .SelectMany(numbers => numbers)     // flatten array
            .Where(n => n % 2 == 0)             // filters stream to even numbers
            .Select(n => n + 0.5)               // maps even numbers to the next odd number as strings
            .ForEachAndComplete(n => sum += n); // add the strings to an array. Also Builds which is forced as the final block.

        // act
        for (int i = 1; i <= 4; i++)
        {
            endingPipeline.Post([i, i + 1]);
        }
        endingPipeline.Complete();
        await endingPipeline.Completion; // no need (or option) to link to this pipeline downstream.// no need (or option) to link to this pipeline downstream.

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
                .Where(x => x % 2 == 0)
                .Build(),
            Array([0, 2, 4])
        ).SetName("where should filter stream");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0)
                .Where(x => x != 4)
                .Build(),
            Array([0, 2])
        ).SetName("builder should support multiple wheres");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => x * 2)
                .Build(),
            Array([0, 2, 4])
        ).SetName("select should transform stream");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => x * 2)
                .Select(x => x + 1)
                .Build(),
            Array([1, 3, 5])
        ).SetName("builder should support multiple selects");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 1) // 1, 3, 5
                .Select(x => x + 1)     // 2, 4, 6
                .Where(x => x < 6)      // 2, 4
                .Select(x => x - 2)     // 0, 2
                .Build(),
            Array([0, 2])
        ).SetName("select and wheres should be interchangeable");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => $"{x}-str")
                .Build(),
            Array(["0-str", "1-str", "2-str"])
        ).SetName("select should support transforming to different types");

        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => $"{x}-str") // int -> str
                .Select(x => new { A = x }) // str -> anon object
                .Build(),
            Array([new { A = "0-str" }, new { A = "1-str" }, new { A = "2-str" }])
        ).SetName("select should support multiple type transforms");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0) // 0, 2, 4
                .Select(x => $"{x}") // "0", "2", "4"
                .Build(),
            Array(["0", "2", "4"])
        ).SetName("should allow where before select to different type");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0) // 0, 2, 4
                .Select(x => $"{x}") // "0", "2", "4"
                .Where(x => int.Parse(x) > 0) // "2", "4"
                .Build(),
            Array(["2", "4"])
        ).SetName("should allow where after select to different type");

        yield return new TestCaseData(
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0) // 0, 2, 4
                .Select(x => $"{x}") // "0", "2", "4"
                .Where(x => int.Parse(x) > 0) // "2", "4"
                .Select(x => int.Parse(x) * 2) // 4, 8
                .Where(x => x > 5) // 8
                .Select(x => x == 8 ? "ate" : "hungry") // "ate"
                .Build(),
            Array(["ate"])
        ).SetName("multiple select, wheres, and transforms, should all work");

        yield return new TestCaseData(
            Array(
                Array(0, 1, 2),
                Array(3, 4, 5)
                ),
            new DataflowBuilder<int[]>()
                .SelectMany(xes => xes)
                .Build(),
            Array(0, 1, 2, 3, 4, 5)
        ).SetName("Should support select manys");

        yield return new TestCaseData(
            Array(0, 1, 2, 3, 4, 5),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0)                 // 0, 2, 4
                .Select(x => new[] {x, x + 1})          // [0, 1], [2, 3], [4, 5]
                .SelectMany(xes => xes)                 // 0, 1, 2, 3, 4, 5
                .Select(x => $"{x}_{x}")                // "0_0", "1_1", "2_2", "3_3", "4_4", "5_5"
                .Select(x => x.Split("_"))              // ["0", "0"], ["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"], ["5", "5"]
                .SelectMany(xes => xes)                 // "0", "0", "1", "1", "2", "2", "3", "3", "4", "4", "5", "5"
                .Where(x => int.Parse(x) > 0)           // "1", "1", "2", "2", "3", "3", "4", "4", "5", "5"
                .Select(x => int.Parse(x) * 2)          // 2, 2, 4, 4, 6, 6, 8, 8, 10, 10
                .Where(x => x > 5)                      // 6, 6, 8, 8, 10, 10
                .Build(),
            Array(6, 6, 8, 8, 10, 10)
        ).SetName("complicated select manys");

        // just checking the runtime of the test
        yield return new TestCaseData(
            Array([0, 1, 2]),
            new DataflowBuilder<int>()
                .ForEachAndComplete(async doneResult => await Task.Delay(100)),
            new DoneResult[] { } // gets consumed by null pointer
        ).SetName("should be able to do async/await for each");


        yield return new TestCaseData(
            Array(0, 1, 2, 3),
            new DataflowBuilder<int>()
                .Select(i => (time: i, value: i * 2))
                .Beam<TestAccumulator>(
                    window: TimeSpan.FromSeconds(2),
                    (stream, acc) => acc.Value += stream.value,
                    (stream, _) => stream.time * 1_000_000_000)
                .Select(acc => (start: acc.WindowStart, end: acc.WindowEnd, value: acc.Value))
                .Build(),
            Array<(long, long, int)>(
                new Window(start: 0_000000000, end: 2_000000000, value: 2 * (0 + 1)),
                new Window(start: 2_000000000, end: 4_000000000, value: 2 * (2 + 3)))
        ).SetName("should be able to beam");
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