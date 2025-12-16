using BlockParty.Blocks.ChunkBlock;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;
internal class ChunkBlockTests
{
    [Test]
    public async Task ShouldHoldOntoMessagesUntilThresholdCrossed()
    {
        // arrange
        var chunkBlock = new ChunkBlock<ulong>(10, i => i);
        var chunks = new List<ulong[]>();
        var gatherBlock = new ActionBlock<ulong[]>(batch => chunks.Add(batch));

        chunkBlock.LinkTo(gatherBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        chunkBlock.Post(5ul);
        chunkBlock.Post(4ul);
        chunkBlock.Post(2ul);
        chunkBlock.Complete();
        await gatherBlock.Completion;

        // assert
        ulong[][] expectedChunks =
        [
            [5, 4],
            [2]
        ];
        Assert.That(chunks, Is.EqualTo(expectedChunks).AsCollection);
    }

    [Test]
    public async Task ShouldEmitOnEqualThreshold()
    {
        // arrange
        var chunkBlock = new ChunkBlock<ulong>(10, i => i);
        var chunks = new List<ulong[]>();
        var gatherBlock = new ActionBlock<ulong[]>(batch => chunks.Add(batch));

        chunkBlock.LinkTo(gatherBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        chunkBlock.Post(5ul);
        chunkBlock.Post(5ul);
        chunkBlock.Complete();
        await gatherBlock.Completion;

        // assert
        ulong[][] expectedChunks =
        [
            [5, 5],
        ];
        Assert.That(chunks, Is.EqualTo(expectedChunks).AsCollection);
    }

    [Test]
    public async Task ShouldEmitOn1Greater()
    {
        // arrange
        var chunkBlock = new ChunkBlock<ulong>(10, i => i);
        var chunks = new List<ulong[]>();
        var gatherBlock = new ActionBlock<ulong[]>(batch => chunks.Add(batch));

        chunkBlock.LinkTo(gatherBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        chunkBlock.Post(10ul);
        chunkBlock.Post(1ul);
        chunkBlock.Complete();
        await gatherBlock.Completion;

        // assert
        ulong[][] expectedChunks =
        [
            [10],
            [1],
        ];
        Assert.That(chunks, Is.EqualTo(expectedChunks).AsCollection);
    }

    [Test]
    public void ShouldExceptionOnItemValueGreaterThanChunkThreshold()
    {
        // arrange
        var chunkBlock = new ChunkBlock<ulong>(10, i => i);

        // act
        chunkBlock.Post(11ul);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await chunkBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Chunk Item has value greater than threshold."));
    }

    [Test]
    public void ShouldPropagateFault()
    {
        // arrange
        var chunkBlock = new ChunkBlock<int>(42, chunkSelector: i => throw new Exception("Test exception"));

        // act
        chunkBlock.Post(42);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await chunkBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test exception"));
    }
}
