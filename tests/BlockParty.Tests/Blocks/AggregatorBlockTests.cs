﻿using BlockParty.Blocks.Aggregator;
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
        var aggregatorBlock = new AggregatorBlock<int, string, (int, string)>(innerBlock, (input, output) =>
        {
            return (input + 1, output);
        });
        var aggregatedValues = new List<(int, string)>();
        var collectorBlock = new ActionBlock<(int, string)>(aggregatedValues.Add);

        aggregatorBlock.LinkTo(collectorBlock, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        aggregatorBlock.Post(1);
        aggregatorBlock.Post(2);
        aggregatorBlock.Post(3);
        aggregatorBlock.Complete();
        await collectorBlock.Completion;

        // assert
        Assert.That(aggregatedValues, Has.Count.EqualTo(3));
        Assert.That(aggregatedValues[0], Is.EqualTo((2, "1")));
        Assert.That(aggregatedValues[1], Is.EqualTo((3, "2")));
        Assert.That(aggregatedValues[2], Is.EqualTo((4, "3")));
    }

    [Test]
    public void ShouldPropagateFaultFromAggregator()
    {
        // arrange
        var innerBlock = new TransformBlock<int, string>(i =>
        {
            return $"{i}";
        });
        var aggregatorBlock = new AggregatorBlock<int, string, (int, string)>(innerBlock, (input, output) =>
        {
            if (true) throw new Exception("Test Exception");
            return (input + 1, output);
        });

        // act
        aggregatorBlock.Post(1);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await aggregatorBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test Exception"));
    }

    [Test]
    public void ShouldPropagateFaultFromInnerBlock()
    {
        // arrange
        var innerBlock = new TransformBlock<int, string>(i =>
        {
            if (true) throw new Exception("Test Exception");
            return $"{i}";
        });
        var aggregatorBlock = new AggregatorBlock<int, string, (int, string)>(innerBlock, (input, output) =>
        {
            return (input + 1, output);
        });

        // act
        aggregatorBlock.Post(1);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await aggregatorBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test Exception"));
    }
}
