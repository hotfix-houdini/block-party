using BlockParty.Blocks.Aggregator;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;
public class AggregatorBlockTests
{
    [Test]
    public async Task ShouldAggregateInputAndOutputToFinalValue()
    {
        // arrange
        var innerBlock = new TransformBlock<int, string>(i =>
        {
            return $"{i}";
        });
        var aggregagtorBlock = new AggregatorBlock<int, string, (int, string)>(innerBlock, (input, output) =>
        {
            return (input + 1, output);
        });
        var aggregatedValues = new List<(int, string)>();
        var collectorBlock = new ActionBlock<(int, string)>(aggregatedValues.Add);

        aggregagtorBlock.LinkTo(collectorBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        aggregagtorBlock.Post(1);
        aggregagtorBlock.Post(2);
        aggregagtorBlock.Post(3);
        aggregagtorBlock.Complete();
        await collectorBlock.Completion;

        // assert
        Assert.That(aggregatedValues, Has.Count.EqualTo(3));
        Assert.That(aggregatedValues[0], Is.EqualTo((2, "1")));
        Assert.That(aggregatedValues[1], Is.EqualTo((3, "2")));
        Assert.That(aggregatedValues[2], Is.EqualTo((4, "3")));
    }
}
