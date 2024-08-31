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

    public static IEnumerable<TestCaseData> DataPipelineShouldFlowTestCases()
    {
        yield return new TestCaseData(
            array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0)
                .Build(),
            array([0, 2, 4])
        ).SetName("where should filter stream");

        yield return new TestCaseData(
            array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 0)
                .Where(x => x != 4)
                .Build(),
            array([0, 2])
        ).SetName("builder should support multiple wheres");

        yield return new TestCaseData(
            array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => x * 2)
                .Build(),
            array([0, 2, 4])
        ).SetName("select should transform stream");

        yield return new TestCaseData(
            array([0, 1, 2]),
            new DataflowBuilder<int>()
                .Select(x => x * 2)
                .Select(x => x + 1)
                .Build(),
            array([1, 3, 5])
        ).SetName("builder should support multiple selects");

        yield return new TestCaseData(
            array([0, 1, 2, 3, 4, 5]),
            new DataflowBuilder<int>()
                .Where(x => x % 2 == 1) // 1, 3, 5
                .Select(x => x + 1)     // 2, 4, 6
                .Where(x => x < 6)      // 2, 4
                .Select(x => x - 2)     // 0, 2,
                .Build(),
            array([0, 2])
        ).SetName("select and wheres should be interchangeable");

        // todo re-introduce when builder is data-agnostic as a first-class-citizen
        //yield return new TestCaseData(
        //    array([0, 1, 2]),
        //    builder<int>(b => b
        //        .Select(x => $"{x}-str")),
        //    array(["0-str", "1-str", "2-str"])
        //).SetName("select should support transforming to different types");
    }

    //[Test]
    //public async Task Select_ShouldAllowTransformsToDifferentTypes()
    //{
    //    // arrange
    //    var sourceStream = BufferBlockFromList([0, 1, 2]);

    //    // act
    //    var pipeline = new DataflowBuilder<int>(sourceStream)
    //        .Select(x => $"{x}-str")
    //        .Build();
    //    var results = await ReadAllAsync(pipeline);

    //    // assert
    //    string[] expected = ["0-str", "1-str", "2-str"];
    //    CollectionAssert.AreEqual(expected, results);
    //}

    //[Test]
    //public async Task SelectWheresAndTransformsShouldAllWork()
    //{
    //    // arrange
    //    var sourceStream = BufferBlockFromList([0, 1, 2, 3, 4, 5]);

    //    // act
    //    // make build produce an IPropagator block?
    //    // have an overload for build that's "Build For" source stream?? so we don't have to do the linking and what not.p

    //    //var pipeline = new DataflowBuilder<int>(sourceStream)
    //    var pipeline = new DataflowBuilder<int>()
    //        .Where(x => x % 2 == 0) // 0, 2, 4
    //        .Select(x => $"{x}") // "0", "2", "4"
    //        .Where(x => int.Parse(x) > 0) // "2", "4"
    //        .Select(x => int.Parse(x) * 2) // 4, 8
    //        .Where(x => x > 5) // 8
    //        .Select(x => x == 8 ? "ate" : "hungry") // "ate"
    //        .Build();
    //    var results = await ReadAllAsync(pipeline);

    //    // assert
    //    string[] expected = ["ate"];
    //    CollectionAssert.AreEqual(expected, results);
    //}

    private ISourceBlock<T> CompletedBufferBlockFromList<T>(IEnumerable<T> collection)
    {
        var block = new BufferBlock<T>();
        foreach (var item in collection)
        {
            block.Post(item);
        }
        block.Complete();

        return block;
    }

    private async Task<List<TOutput>> ReadAllAsync<TInput, TOutput>(IPropagatorBlock<TInput, TOutput> block)
    {
        var bufferBlock = new BufferBlock<TOutput>();

        // Link the propagator block to the buffer block
        block.LinkTo(bufferBlock, new DataflowLinkOptions { PropagateCompletion = true });

        var results = new List<TOutput>();
        await foreach (var item in bufferBlock.ReceiveAllAsync())
        {
            results.Add(item);
        }

        await block.Completion;
        return results;
    }

    private static T[] array<T>(params T[] elements) => elements;
}
