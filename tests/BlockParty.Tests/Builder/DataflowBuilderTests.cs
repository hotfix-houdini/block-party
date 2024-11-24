using BlockParty.Builder;
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
                .Select(x => x - 2)     // 0, 2,
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
            Array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0) // 0, 2, 4
                .FanoutWithJoin(
                    chain1 => chain1.Select(x => x * 2),        // 0, 4, 8
                    chain2 => chain2.Select(x => x.ToString())  // "0", "2", "4"
                    ) // (0, "0"), (4, "2"), (8, "4")
                .Build(),
            Array([(0, "0"), (4, "2"), (8, "4")])
        ).SetName("fanout-with-join-2 should split and join");
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