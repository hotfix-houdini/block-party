using BlockParty.Blocks.Throttle;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;

public class ThrottleBlockTests
{
    [Test]
    public void ShouldEmitASingleValueWithinThrottle()
    {
        // arrange
        var throttleBlock = new ThrottleBlock<int>(TimeSpan.FromMilliseconds(250));
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
        throttleBlock.Complete();
        targetBlock.Completion.Wait();

        // assert
        Assert.That(receivedMessages, Has.Count.EqualTo(1));
        Assert.That(receivedMessages.Single().value, Is.EqualTo(1));
        Assert.That(receivedMessages.Single().receivedTime, Is.LessThan(startTime.AddMilliseconds(300)));
    }

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

    [Test]
    public void ShouldCompleteWithNothingInTheQueue()
    {
        // arrange
        var throttleBlock = new ThrottleBlock<int>(TimeSpan.FromSeconds(1));
        var targetBlock = new ActionBlock<int>(i =>
        {

        });
        var linkOptions = new DataflowLinkOptions()
        {
            PropagateCompletion = true
        };
        throttleBlock.LinkTo(targetBlock, linkOptions);

        // act
        var startTime = DateTimeOffset.UtcNow;
        throttleBlock.Complete();
        targetBlock.Completion.Wait();

        // assert
        var endTime = DateTimeOffset.UtcNow;
        Assert.That((endTime - startTime).TotalMilliseconds, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(1).TotalMilliseconds).Within(TimeSpan.FromMilliseconds(100).TotalMilliseconds));
    }

    [Test]
    public void ShouldStaggerAndWaitFor3InARow()
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
        throttleBlock.Post(3);
        throttleBlock.Complete();
        targetBlock.Completion.Wait();

        // assert
        Assert.That(receivedMessages, Has.Count.EqualTo(3));
        Assert.That(receivedMessages.First().value, Is.EqualTo(1));
        Assert.That(receivedMessages.First().receivedTime.Ticks, Is.LessThanOrEqualTo(startTime.AddSeconds(1).Ticks).Within(TimeSpan.FromMilliseconds(100).Ticks));
        Assert.That(receivedMessages.Skip(1).First().value, Is.EqualTo(2));
        Assert.That((receivedMessages.Skip(1).First().receivedTime - receivedMessages.First().receivedTime).Ticks, Is.GreaterThanOrEqualTo(period.Ticks).Within(TimeSpan.FromMilliseconds(100).Ticks));
        Assert.That(receivedMessages.Skip(2).First().value, Is.EqualTo(3));
        Assert.That((receivedMessages.Skip(2).First().receivedTime - receivedMessages.First().receivedTime).Ticks, Is.GreaterThanOrEqualTo(period.Ticks * 2).Within(TimeSpan.FromMilliseconds(100).Ticks));
    }

    [Test]
    public void ShouldProcessBlocksBeforeCompletition()
    {
        // arrange
        var period = TimeSpan.FromMilliseconds(250);
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
        throttleBlock.Post(1);
        throttleBlock.Post(2);
        throttleBlock.Post(3);
        Thread.Sleep(1000);

        // assert
        try
        {
            Assert.That(receivedMessages, Has.Count.EqualTo(3));
        }
        finally
        {
            throttleBlock.Complete();
            targetBlock.Completion.Wait();
        }
    }

    [Test]
    public void ShouldThrowExceptionIfPeriodLessThan100Milliseconds()
    {
        // arrange
        var period = TimeSpan.FromMicroseconds(100);

        // act
        var exception = Assert.Throws<ArgumentException>(() => new ThrottleBlock<int>(period));

        // assert
        Assert.That(exception?.Message, Is.EqualTo("Period must be greater than or equal to 100 milliseconds. (Parameter 'throttle')"));
    }
}