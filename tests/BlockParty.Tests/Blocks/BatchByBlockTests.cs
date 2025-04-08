using BlockParty.Blocks.GroupBy;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;

public class BatchByBlockTests
{
    [Test]
    public async Task ShouldHoldOntoMessagesInSameGroupUntilNextGroup()
    {
        // arrange
        var batchByBlock = new BatchByBlock<int, int>(selector: i => i / 2);
        var batches = new List<int[]>();
        var gatherBlock = new ActionBlock<int[]>(batch => batches.Add(batch));

        batchByBlock.LinkTo(gatherBlock, new DataflowLinkOptions(){  PropagateCompletion = true });

        // act
        batchByBlock.Post(1);
        batchByBlock.Post(2);
        batchByBlock.Post(3);
        batchByBlock.Complete();
        await gatherBlock.Completion;

        // assert
        int[][] expectedBatches =
        [
            [1],
            [2, 3]
        ];
        Assert.That(batches, Is.EqualTo(expectedBatches).AsCollection);
    }

    [Test]
    public async Task ShouldSupportDifferentTypesAndDifferentContiguousGroups()
    {
        // arrange
        var batchByBlock = new BatchByBlock<string, int>(selector: i => int.Parse(i) / 2);
        var batches = new List<string[]>();
        var gatherBlock = new ActionBlock<string[]>(batch => batches.Add(batch));

        batchByBlock.LinkTo(gatherBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        batchByBlock.Post("1");
        batchByBlock.Post("2");
        batchByBlock.Post("3");
        batchByBlock.Post("6");
        batchByBlock.Post("1");
        batchByBlock.Post("1");
        batchByBlock.Complete();
        await gatherBlock.Completion;

        // assert
        string[][] expectedBatches =
        [
            ["1"],
            ["2", "3"],
            ["6"],
            ["1", "1"]
        ];
        Assert.That(batches, Is.EqualTo(expectedBatches).AsCollection);
    }

    [Test]
    public void ShouldPropagateFault()
    {
        // arrange
        var batchByBlock = new BatchByBlock<int, int>(selector: i => throw new Exception("Test exception"));

        // act
        batchByBlock.Post(42);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await batchByBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test exception"));
    }
}
