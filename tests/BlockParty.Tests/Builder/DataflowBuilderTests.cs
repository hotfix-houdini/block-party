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
        var results = await ReadAllAsync(pipeline);

        // assert
        int[] expected = [0, 2, 4];
        CollectionAssert.AreEqual(expected, results);
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
        var results = await ReadAllAsync(pipeline);

        // assert
        int[] expected = [0, 2];
        CollectionAssert.AreEqual(expected, results);
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
        var results = await ReadAllAsync(pipeline);

        // assert
        int[] expected = [0, 2, 4];
        CollectionAssert.AreEqual(expected, results);
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
        var results = await ReadAllAsync(pipeline);

        // assert
        int[] expected = [1, 3, 5];
        CollectionAssert.AreEqual(expected, results);
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
        var results = await ReadAllAsync(pipeline);

        // assert
        int[] expected = [0, 2];
        CollectionAssert.AreEqual(expected, results);
    }

    [Test]
    public async Task Select_ShouldAllowTransformsToDifferentTypes()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Select(x => $"{x}-str")
            .Build();
        var results = await ReadAllAsync(pipeline);

        // assert
        string[] expected = ["0-str", "1-str", "2-str"];
        CollectionAssert.AreEqual(expected, results);
    }

    [Test]
    public async Task SelectWheresAndTransformsShouldAllWork()
    {
        // arrange
        var sourceStream = BufferBlockFromList([0, 1, 2, 3, 4, 5]);

        // act
        var pipeline = new DataflowBuilder<int>(sourceStream)
            .Where(x => x % 2 == 0) // 0, 2, 4
            .Select(x => $"{x}") // "0", "2", "4"
            .Where(x => int.Parse(x) > 0) // "2", "4"
            .Select(x => int.Parse(x) * 2) // 4, 8
            .Where(x => x > 5) // 8
            .Select(x => x == 8 ? "ate" : "hungry") // "ate"
            .Build();
        var results = await ReadAllAsync(pipeline);

        // assert
        string[] expected = ["ate"];
        CollectionAssert.AreEqual(expected, results);
    }

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
}
