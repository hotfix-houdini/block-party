using BlockParty.Blocks.SequencePreserving;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;
public class SequencePreservingBlockTests
{
    [Theory]
    [TestCase("1, 2, 3", null, OnCompleteBufferedMessageBehavior.Discard, "1, 2, 3")]
    [TestCase("3, 2, 1", null, OnCompleteBufferedMessageBehavior.Discard, "3")]
    [TestCase("2, 4, 3", null, OnCompleteBufferedMessageBehavior.Discard, "2, 3, 4")]
    [TestCase("1, 2, 3", 2, OnCompleteBufferedMessageBehavior.Discard, "2, 3")]
    [TestCase("1, 2, 3", 3, OnCompleteBufferedMessageBehavior.Discard, "3")]
    [TestCase("3, 2, 1", 1, OnCompleteBufferedMessageBehavior.Discard, "1, 2, 3")]
    [TestCase("3, 2, 1", 2, OnCompleteBufferedMessageBehavior.Discard, "2, 3")]
    [TestCase("2, 4, 3", 1, OnCompleteBufferedMessageBehavior.Discard, "")]

    [TestCase("1, 2, 3", null, OnCompleteBufferedMessageBehavior.Emit, "1, 2, 3")]
    [TestCase("3, 2, 1", null, OnCompleteBufferedMessageBehavior.Emit, "3")]
    [TestCase("2, 4, 3", null, OnCompleteBufferedMessageBehavior.Emit, "2, 3, 4")]
    [TestCase("1, 2, 3", 2, OnCompleteBufferedMessageBehavior.Emit, "2, 3")]
    [TestCase("1, 2, 3", 3, OnCompleteBufferedMessageBehavior.Emit, "3")]
    [TestCase("3, 2, 1", 1, OnCompleteBufferedMessageBehavior.Emit, "1, 2, 3")]
    [TestCase("3, 2, 1", 2, OnCompleteBufferedMessageBehavior.Emit, "2, 3")]

    [TestCase("2, 4, 3", 1, OnCompleteBufferedMessageBehavior.Emit, "2, 3, 4")]
    [TestCase("2, 4, 6, 5, 7", 1, OnCompleteBufferedMessageBehavior.Emit, "2, 4, 5, 6, 7")]
    public async Task ShouldReorderStreamToBeInOrder(
        string inputStreamCsv,
        long? fromFirstElement,
        OnCompleteBufferedMessageBehavior onCompleteBehavior,
        string expectedOutputStreamCsv)
    {
        // arrange
        var expectedOutput = expectedOutputStreamCsv.Split(", ").Where(x => !string.IsNullOrWhiteSpace(x)).ToList();
        var actualItems = new List<string>();
        var sequenceInitialization = fromFirstElement == null
            ? SequenceInitialization.FromFirstElement
            : SequenceInitialization.From(fromFirstElement.Value);
        var settings = new SequencePreservingBlockSettings()
        {
            SequenceInitialization = sequenceInitialization,
            OnCompleteBufferedMessageBehavior = onCompleteBehavior,
        };

        var inputBlock = new TransformManyBlock<string, string>(inputString => inputString.Split(", "));
        var sut = new SequencePreservingBlock<string>(item => long.Parse(item), settings);
        var actualCollectorBlock = new ActionBlock<string>(item => actualItems.Add(item));

        var dataflowLinkOptions = new DataflowLinkOptions() {  PropagateCompletion = true };
        inputBlock.LinkTo(sut, dataflowLinkOptions);
        sut.LinkTo(actualCollectorBlock, dataflowLinkOptions);

        // act
        inputBlock.Post(inputStreamCsv);
        inputBlock.Complete();
        await actualCollectorBlock.Completion;

        // assert
        Assert.That(actualItems, Is.EqualTo(expectedOutput).AsCollection);
    }

    [Test]
    public async Task ShouldReorderStreamToBeInOrderSimple()
    {
        // arrange
        var settings = new SequencePreservingBlockSettings()
        {
            SequenceInitialization = SequenceInitialization.From(1),
            OnCompleteBufferedMessageBehavior = OnCompleteBufferedMessageBehavior.Discard,
        };
        var sequencePreservingBlock = new SequencePreservingBlock<string>(sequenceIndexExtractor: (item) => long.Parse(item), settings);

        var actualItems = new List<string>();
        var collectorBlock = new ActionBlock<string>(item => actualItems.Add(item));
        sequencePreservingBlock.LinkTo(collectorBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        sequencePreservingBlock.Post("3");
        sequencePreservingBlock.Post("1");
        sequencePreservingBlock.Post("2");
        sequencePreservingBlock.Complete();
        await collectorBlock.Completion;

        // assert
        Assert.That(actualItems, Is.EqualTo(["1", "2", "3"]).AsCollection);
    }

    [Test]
    public void ShouldPropagateFault()
    {
        // arrange
        var settings = new SequencePreservingBlockSettings()
        {
            SequenceInitialization = SequenceInitialization.FromFirstElement,
            OnCompleteBufferedMessageBehavior = OnCompleteBufferedMessageBehavior.Discard,
        };
        var sequencePreservingBlock = new SequencePreservingBlock<string>(
            sequenceIndexExtractor: (item) =>
            {
                throw new Exception("Test exception");
                return long.Parse(item); 
            }, settings);

        // act
        sequencePreservingBlock.Post("3");
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await sequencePreservingBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test exception"));
    }
}
