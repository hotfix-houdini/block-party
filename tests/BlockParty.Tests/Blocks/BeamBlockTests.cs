using BlockParty.Blocks.Beam;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;

public class BeamBlockTests
{
    [Test]
    public void ShouldAccumulate()
    {
        // arrange
        var beamBlock = new BeamBlock<TestModel, TestAccumulator>(
            TimeSpan.FromHours(1),
            (streamed, accumulator) =>
            {
                accumulator.Value += streamed.Value;
            },
            (streamed) => streamed.Time);
        var accumulators = new List<TestAccumulator>();
        var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

        beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
        {
            PropagateCompletion = true
        });

        var streamed1 = new TestModel() { Time = 1, Value = 1 };
        var streamed2 = new TestModel() { Time = 2, Value = 2 };
        var streamed3 = new TestModel() { Time = 3600_000000000, Value = 2 };

        // act
        beamBlock.Post(streamed1);
        beamBlock.Post(streamed2);
        beamBlock.Post(streamed3);
        beamBlock.Complete();
        gatherBlock.Completion.Wait();

        // assert
        Assert.Multiple(() =>
        {
            Assert.That(accumulators, Has.Count.EqualTo(2));
            Assert.That(accumulators.First().Value, Is.EqualTo(3));
            Assert.That(accumulators.Last().Value, Is.EqualTo(2));
        });
    }

    [Test]
    public void ShouldAccumulateMultipleWindows()
    {
        // arrange
        var beamBlock = new BeamBlock<TestModel, TestAccumulator>(
            TimeSpan.FromHours(1),
            (streamed, accumulator) =>
            {
                accumulator.Value += streamed.Value;
            },
            (streamed) => streamed.Time);
        var accumulators = new List<TestAccumulator>();
        var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

        beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
        {
            PropagateCompletion = true
        });

        var streamedModels = new List<TestModel>()
        {
            new() { Time = 0, Value = 1 },
            new() { Time = 1800_000000000, Value = 2 },
            new() { Time = 3599_000000000, Value = 2 },
            new() { Time = 3601_000000000, Value = 3 },
            new() { Time = 7200_000000000, Value = 3 },
        };

        // act
        foreach (var stream in streamedModels)
        {
            beamBlock.Post(stream);
        }
        beamBlock.Complete();
        gatherBlock.Completion.Wait();

        // assert
        Assert.Multiple(() =>
        {
            Assert.That(accumulators, Has.Count.EqualTo(3));
            Assert.That(accumulators[0].Value, Is.EqualTo(5));
            Assert.That(accumulators[1].Value, Is.EqualTo(3));
            Assert.That(accumulators[2].Value, Is.EqualTo(3));
        });
    }

    [Test]
    public void ShouldNotEmitUnitializedAccumulatorOnCompletition() // no watermark here
    {
        // arrange
        var beamBlock = new BeamBlock<TestModel, TestAccumulator>(
            TimeSpan.FromHours(1),
            (streamed, accumulator) =>
            {
                accumulator.Value += streamed.Value;
            },
            (streamed) => streamed.Time);
        var accumulators = new List<TestAccumulator>();
        var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

        beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
        {
            PropagateCompletion = true
        });

        // act
        beamBlock.Complete();
        gatherBlock.Completion.Wait();

        // assert
        Assert.That(accumulators, Has.Count.EqualTo(0));
    }

    [Test]
    public void ShouldIncludeWindowTimeInAccumulator()
    {
        // arrange
        var beamBlock = new BeamBlock<TestModel, TestAccumulator>(
            TimeSpan.FromHours(1),
            (streamed, accumulator) =>
            {
                accumulator.Value += streamed.Value;
            },
            (streamed) => streamed.Time);
        var accumulators = new List<TestAccumulator>();
        var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

        beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
        {
            PropagateCompletion = true
        });

        var streamedModels = new List<TestModel>()
        {
            new() { Time = 0, Value = 1 },
            new() { Time = 1800_000000000, Value = 2 },
            new() { Time = 3599_000000000, Value = 2 },
            new() { Time = 3600_000000000, Value = 3 },
            new() { Time = 7200_000000000, Value = 3 },
            new() { Time = 10811_000000000, Value = 3 },
        };

        // act
        foreach (var stream in streamedModels)
        {
            beamBlock.Post(stream);
        }
        beamBlock.Complete();
        gatherBlock.Completion.Wait();

        // assert
        Assert.Multiple(() =>
        {
            Assert.That(accumulators, Has.Count.EqualTo(4));
            Assert.That(accumulators[0].WindowStart, Is.EqualTo(0));
            Assert.That(accumulators[0].WindowEnd, Is.EqualTo(3600_000000000));
            Assert.That(accumulators[1].WindowStart, Is.EqualTo(3600_000000000));
            Assert.That(accumulators[1].WindowEnd, Is.EqualTo(7200_000000000));
            Assert.That(accumulators[2].WindowStart, Is.EqualTo(7200_000000000));
            Assert.That(accumulators[2].WindowEnd, Is.EqualTo(10800_000000000));
            Assert.That(accumulators[3].WindowStart, Is.EqualTo(10800_000000000));
            Assert.That(accumulators[3].WindowEnd, Is.EqualTo(14400_000000000));
        });
    }

    [Test]
    public void ShouldFillInBlankWindowsIfDetected()
    {
        // arrange
        var beamBlock = new BeamBlock<TestModel, TestAccumulator>(
            TimeSpan.FromHours(1),
            (streamed, accumulator) =>
            {
                accumulator.Value += streamed.Value;
            },
            (streamed) => streamed.Time);
        var accumulators = new List<TestAccumulator>();
        var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

        beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
        {
            PropagateCompletion = true
        });

        var streamedModels = new List<TestModel>()
        {
            new() { Time = 0, Value = 1 },
            new() { Time = 1800_000000000, Value = 2 },
            new() { Time = 3599_000000000, Value = 2 },
            new() { Time = 3601_000000000, Value = 3 },
            new() { Time = 7200_000000000, Value = 3 },
            new() { Time = 18000_000000000, Value = 4 },
        };

        // act
        foreach (var stream in streamedModels)
        {
            beamBlock.Post(stream);
        }
        beamBlock.Complete();
        gatherBlock.Completion.Wait();

        // assert
        Assert.Multiple(() =>
        {
            Assert.That(accumulators, Has.Count.EqualTo(6));
            Assert.That(accumulators[0].Value, Is.EqualTo(5));
            Assert.That(accumulators[1].Value, Is.EqualTo(3));
            Assert.That(accumulators[2].Value, Is.EqualTo(3));
            Assert.That(accumulators[3].Value, Is.EqualTo(0));
            Assert.That(accumulators[3].WindowStart, Is.EqualTo(10800_000000000));
            Assert.That(accumulators[3].WindowEnd, Is.EqualTo(14400_000000000));
            Assert.That(accumulators[4].Value, Is.EqualTo(0));
            Assert.That(accumulators[4].WindowStart, Is.EqualTo(14400_000000000));
            Assert.That(accumulators[4].WindowEnd, Is.EqualTo(18000_000000000));
            Assert.That(accumulators[5].Value, Is.EqualTo(4));
            Assert.That(accumulators[5].WindowStart, Is.EqualTo(18000_000000000));
            Assert.That(accumulators[5].WindowEnd, Is.EqualTo(21600_000000000));
        });
    }
}

public class TestModel
{
    public long Time { get; set; }
    public int Value { get; set; }
}

public class TestAccumulator : IAccumulator
{
    public int Value { get; set; }
    public long WindowStart { get; set; }
    public long WindowEnd { get; set; }
}