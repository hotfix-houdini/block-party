# block-party
Extensions of the .NET TPL Dataflow Library

# Blocks
### BeamBlock
Inspired by Apache Beam, BeamBlock lets you group an incoming stream into time windows, and then aggregate each item within a window into an arbitrary accumulator. Downstream blocks will receive these windowed accumulators.

Conceptual Example:
```txt
Input Stream: 
[
	(time: 1pm, value: 1),
	(time: 2pm, value: 2),
	(time: 3pm, value: 3)
]
Accumulator: window(int numItems, int valueOfItems)

Beam Block on Input Stream:
	- window size: 2 hours
	- summary method: (item, window) => { window.numItems++; window.valueOfItems += item.value; }

Output Stream:
[
	(numItems: 2, valueOfItems: 3), // time 1pm and time 2pm
	(numItems: 1, valueOfItems: 3) // time 3pm
]
```

### ThrottleBlock
ThrottleBlock can be inserted into a TPL Dataflow pipeline to insert a wall-clock-time bottleneck in the stream. Useful when a downstream resource has a rate limit.

Conceptual Example:
```txt
Input Stream: 
[
	(value: 1), // received at 1:00pm
	(value: 2), // received at 1:01pm
	(value: 3)  // received at 1:02pm 
]

Throttle block on InputStream:
	- Throttle to 1 emitted per 2 minutes.

Output Stream:
[
	(value: 1), // emitted at 1:00pm
	(value: 2), // emitted at 1:02pm
	(value: 3)  // emitted at 1:04pm 
]
```


# Samples
### BeamBlock
```csharp
public class TestModelWithDateTimeOffsetTime
{
    public DateTimeOffset Time { get; set; }
    public int Value { get; set; }
}

public class TestAccumulator : IAccumulator
{
    public int Value { get; set; }
    public long WindowStart { get; set; }
    public long WindowEnd { get; set; }
}

[Test]
public void ShouldAccumulateItemsIntoWindows()
{
    // arrange
    var beamBlock = new BeamBlock<TestModelWithDateTimeOffsetTime, TestAccumulator>(
        TimeSpan.FromHours(1),
        (streamed, accumulator) =>
        {
            accumulator.Value += streamed.Value;
        },
        (streamed, timeConverter) => timeConverter.ConvertToNanosecondEpoch(streamed.Time));
    var accumulators = new List<TestAccumulator>();
    var gatherBlock = new ActionBlock<TestAccumulator>(i => accumulators.Add(i));

    beamBlock.LinkTo(gatherBlock, new DataflowLinkOptions()
    {
        PropagateCompletion = true
    });

    var streamed1 = new TestModelWithDateTimeOffsetTime() { Time = DateTimeOffset.Parse("2024-03-28T07:00:01-05:00"), Value = 1 };
    var streamed2 = new TestModelWithDateTimeOffsetTime() { Time = DateTimeOffset.Parse("2024-03-28T07:00:02-05:00"), Value = 2 };
    var streamed3 = new TestModelWithDateTimeOffsetTime() { Time = DateTimeOffset.Parse("2024-03-28T08:00:00-05:00"), Value = 2 };

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
```

### Throttle Block 
```csharp
[Test]
public void ShouldWaitForThrottleToElapseFor2ndEmit()
{
    // arrange
    var period = TimeSpan.FromSeconds(1);
    var throttleBlock = new ThrottleBlock<int>(period);
    var receivedMessages = new List<(int value, DateTimeOffset receivedTime)>();
    var targetBlock = new ActionBlock<int>(i =>
    {
        receivedMessages.Add((i, DateTimeOffset.UtcNow));
    });
    var linkOptions = new DataflowLinkOptions()
    {
        PropagateCompletion = true
    };
    throttleBlock.LinkTo(targetBlock, linkOptions);

    // act
    var startTime = DateTimeOffset.UtcNow;
    throttleBlock.Post(1);
    throttleBlock.Post(2);
    throttleBlock.Complete();
    targetBlock.Completion.Wait();

    // assert
    Assert.That(receivedMessages, Has.Count.EqualTo(2));
    Assert.That(receivedMessages.First().value, Is.EqualTo(1));
    Assert.That(receivedMessages.First().receivedTime.Ticks, Is.LessThanOrEqualTo(startTime.AddSeconds(1).Ticks).Within(TimeSpan.FromMilliseconds(100).Ticks));
    Assert.That(receivedMessages.Last().value, Is.EqualTo(2));
    Assert.That((receivedMessages.Last().receivedTime - receivedMessages.First().receivedTime).Ticks, Is.GreaterThanOrEqualTo(period.Ticks).Within(TimeSpan.FromMilliseconds(100).Ticks));
}
```

# Settings
### BeamBlock
- OmitIncompleteFinalWindow = `true` or `false`. Example:
  ```txt
    Input Stream: 
    [
	    (time: 1pm, value: 1),
	    (time: 2pm, value: 2),
	    (time: 3pm, value: 3)
    ]
    Accumulator: window(int numItems, int valueOfItems)

    Beam Block on Input Stream:
	    - window size: 2 hours
	    - summary method: (item, window) => { window.numItems++; window.valueOfItems += item.value; }

    Output Stream with OmitIncompleteFinalWindow = false (default):
    [
	    (numItems: 2, valueOfItems: 3), // time 1pm and time 2pm
	    (numItems: 1, valueOfItems: 3) // time 3pm
    ] 

    Output Stream with OmitIncompleteFinalWindow = true:
    [
	    (numItems: 2, valueOfItems: 3), // time 1pm and time 2pm; emitted on item3
        // item 3 not emitted; didn't receive an item >= 4pm.
    ]
  ```