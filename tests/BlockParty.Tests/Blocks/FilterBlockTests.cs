using BlockParty.Blocks.Filter;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Blocks;
public class FilterBlockTests
{
    [Test]
    [TestCase("1, 2, 3", 0, "1, 2, 3")]
    [TestCase("1, 2, 3", 1, "2, 3")]
    [TestCase("1, 2, 3", 2, "3")]
    [TestCase("1, 2, 3", 3, "")]
    [TestCase("1, 2, 3", -1, "1, 2, 3")]
    public async Task ShouldFilter(string inputStream, int lowerBound, string expectedOutputStream)
    {
        // arrange
        var inputs = inputStream.Split(", ").Select(x => int.Parse(x)).ToList();
        var expectedOutputs = expectedOutputStream.Split(", ").Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => int.Parse(x)).ToList();
        var filterBlock = new FilterBlock<int>(x => x > lowerBound);

        var actualOutputs = new List<int>();
        var outputCollector = new ActionBlock<int>(x => actualOutputs.Add(x));

        filterBlock.LinkTo(outputCollector, new DataflowLinkOptions() {  PropagateCompletion = true });

        // act
        foreach (var input in inputs)
        {
            filterBlock.Post(input);
        }
        filterBlock.Complete();
        await outputCollector.Completion;

        // assert
        Assert.That(actualOutputs, Is.EqualTo(expectedOutputs).AsCollection);
    }

    [Test]
    public async Task ShouldFilterSimple()
    {
        // arrange
        var filterBlock = new FilterBlock<int>(x => x % 2 == 0); // even #'s only

        var actualOutputs = new List<int>();
        var outputCollector = new ActionBlock<int>(x => actualOutputs.Add(x));
        filterBlock.LinkTo(outputCollector, new DataflowLinkOptions() { PropagateCompletion = true });

        // act
        for (int i = 0; i < 10; i++)
        {
            filterBlock.Post(i);
        }
        filterBlock.Complete();
        await outputCollector.Completion;

        // assert
        Assert.That(actualOutputs, Is.EqualTo([0, 2, 4, 6, 8]).AsCollection);
    }

    [Test]
    public async Task Filter_ShouldPropagateFault()
    {
        // arrange
        var filterBlock = new FilterBlock<int>(x => 
        {
            if (true)
            {
                throw new Exception("Test exception");
            }
            return true;
        });

        // act
        filterBlock.Post(1);
        var potentialException = Assert.ThrowsAsync<AggregateException>(async () => await filterBlock.Completion);

        // assert
        Assert.That(potentialException, Is.Not.Null);
        Assert.That(potentialException.Message, Does.Contain("Test exception"));
    }
}