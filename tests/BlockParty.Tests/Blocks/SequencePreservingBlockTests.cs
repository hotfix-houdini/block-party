using BlockParty.Blocks.SequencePreserving;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;
public class SequencePreservingBlockTests
{
    [Theory]
    [TestCase("1, 2, 3", null, "1, 2, 3")]
    [TestCase("3, 2, 1", null, "3")]
    [TestCase("2, 4, 3", null, "2, 3, 4")]
    [TestCase("1, 2, 3", 2, "2, 3")]
    [TestCase("1, 2, 3", 3, "3")]
    [TestCase("3, 2, 1", 1, "1, 2, 3")]
    [TestCase("3, 2, 1", 2, "2, 3")]
    [TestCase("2, 4, 3", 1, "")]
    public async Task ShouldReorderStreamToBeInOrder(string inputStreamCsv, long? fromFirstElement, string expectedOutputStreamCsv)
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
        CollectionAssert.AreEqual(expectedOutput, actualItems);
    }

    // should emit out-of-order on completition or nah option 
    // failed to post exception
}
