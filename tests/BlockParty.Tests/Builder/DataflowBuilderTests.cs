using BlockParty.Builder;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Builder;
public class DataflowBuilderTests
{
    [Test]
    public async Task Where_ShouldFilterStream()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2, 3, 4, 5]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream) // todo, figure out if we accept the source stream in a builder, OR, can simply make like a "make the next block" 
            .Where(x => x % 2 == 0)
            .Build();
        var downStream = await ReadAllAsync(pipeline);

        // assert
        Assert.That(downStream, Has.Count.EqualTo(3));
        Assert.That(downStream[0], Is.EqualTo(0));
        Assert.That(downStream[1], Is.EqualTo(2));
        Assert.That(downStream[2], Is.EqualTo(4));
    }

    [Test]
    public async Task Where_ShouldSupportMultpleWheres()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2, 3, 4, 5]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Where(x => x % 2 == 0)
            .Where(x => x != 4)
            .Build();
        var downStream = await ReadAllAsync(pipeline);

        // assert
        Assert.That(downStream, Has.Count.EqualTo(2));
        Assert.That(downStream[0], Is.EqualTo(0));
        Assert.That(downStream[1], Is.EqualTo(2));
    }

    [Test]
    public async Task Select_ShoulTransformStream()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Select(x => x * 2)
            .Build();
        var downStream = await ReadAllAsync(pipeline);

        // assert
        Assert.That(downStream, Has.Count.EqualTo(3));
        Assert.That(downStream[0], Is.EqualTo(0));
        Assert.That(downStream[1], Is.EqualTo(2));
        Assert.That(downStream[2], Is.EqualTo(4));
    }

    [Test]
    public async Task Select_ShouldSupportMultiple()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Select(x => x * 2)
            .Select(x => x + 1)
            .Build();
        var downStream = await ReadAllAsync(pipeline);

        // assert
        Assert.That(downStream, Has.Count.EqualTo(3));
        Assert.That(downStream[0], Is.EqualTo(1));
        Assert.That(downStream[1], Is.EqualTo(3));
        Assert.That(downStream[2], Is.EqualTo(5));
    }

    [Test]
    public async Task SelectAndWhere_ShouldBeInterchangeable()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2, 3, 4, 5]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Where(x => x % 2 == 1) // 1, 3, 5
            .Select(x => x + 1) // 2, 4, 6
            .Where(x => x < 6) // 2, 4
            .Select(x => x - 2) // 0, 2
            .Build();
        var downStream = await ReadAllAsync(pipeline);

        // assert
        Assert.That(downStream, Has.Count.EqualTo(2));
        Assert.That(downStream[0], Is.EqualTo(0));
        Assert.That(downStream[1], Is.EqualTo(2));
    }

    // should allow transforms to different types

    private ISourceBlock<T> BufferBlockFromList<T>(List<T> list)
    {
        var block = new BufferBlock<T>();
        foreach (var item in list)
        {
            block.Post(item);
        }
        block.Complete();

        return block;
    }

    private async Task<List<T>> ReadAllAsync<T>(BufferBlock<T> bufferBlock)
    {
        var results = new List<T>();
        await foreach (var item in bufferBlock.ReceiveAllAsync())
        {
            results.Add(item);
        }
        await bufferBlock.Completion;
        return results;
    }

    // should propagate completition 
    // todo make awesome test-builder/test cases. (i.e. stream inputs, the chain, then expected outputstreams)
    
    // indefinite chains (many different wheres and transforms)
}
