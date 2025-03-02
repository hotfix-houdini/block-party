using BlockParty.Builder.DAG;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Tests.Builder.DAG;

public class DataflowDAGTests
{
    [Theory]
    [TestCaseSource(nameof(DagBuilderTestCases))]
    public void DagBuilderShouldCreateExpectedGraph<TInput, TOutput>(DataflowDAG<TInput, TOutput> dag, string expectedMermaidGraph)
    {
        // act
        var graph = dag.GenerateMermaidGraph();

        // assert
        Assert.That(graph, Is.EqualTo(expectedMermaidGraph));
    }

    private static IEnumerable<TestCaseData> DagBuilderTestCases()
    {
        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new BufferBlock<int>()),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]

```
").SetName("dag builder should create a 1 block graph");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new BufferBlock<int>())
                .Add(new TransformBlock<int, int>(i => i)),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  transform_1[""TransformBlock&lt;Int32,Int32&gt;""]

  buffer_0 --> transform_1
```
").SetName("dag builder should create a 2 block graph");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new BufferBlock<int>())
                .Add(new TransformBlock<int, string>(i => $"{i}"))
                .Add(new BatchBlock<string>(42)),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  transform_1[""TransformBlock&lt;Int32,String&gt;""]
  batch_2[""BatchBlock&lt;String&gt;""]

  buffer_0 --> transform_1
  transform_1 --> batch_2
```
").SetName("dag builder should change types");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new TransformBlock<int, Dictionary<DataflowBuilderTests, List<Stack<string>>>>(i => new Dictionary<DataflowBuilderTests, List<Stack<string>>>())),
@"```mermaid
graph TD
  transform_0[""TransformBlock&lt;Int32,Dictionary&lt;DataflowBuilderTests,List&lt;Stack&lt;String&gt;&gt;&gt;&gt;""]

```
").SetName("dag builder should fully qualify types ");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .AddFanOutAndIn(new BufferBlock<int>(),
                [
                    new DataflowDAG<int>(nodeIdOffset: 1).Add(new TransformBlock<int, string>(i => $"{i}")),
                    new DataflowDAG<int>(nodeIdOffset: 2).Add(new TransformBlock<int, string>(i => $"{i}")),
                ],
                new BufferBlock<string>()),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  transform_1[""TransformBlock&lt;Int32,String&gt;""]
  transform_2[""TransformBlock&lt;Int32,String&gt;""]
  buffer_3[""BufferBlock&lt;String&gt;""]

  buffer_0 --> transform_1
  buffer_0 --> transform_2
  transform_1 --> buffer_3
  transform_2 --> buffer_3
```
").SetName("dag builder should support fanout and ins");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new BufferBlock<int>())
                .AddFanOutAndIn(new BufferBlock<int>(),
                [
                    new DataflowDAG<int>(nodeIdOffset: 2).Add(new TransformBlock<int, string>(i => $"{i}")).Add(new BatchBlock<string>(2)),
                    new DataflowDAG<int>(nodeIdOffset: 4).Add(new TransformBlock<int, string>(i => $"{i}")).Add(new BatchBlock<string>(2)),
                ],
                new BufferBlock<string[]>())
                .Add(new TransformManyBlock<string[], string>(strings => strings)),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  buffer_1[""BufferBlock&lt;Int32&gt;""]
  transform_2[""TransformBlock&lt;Int32,String&gt;""]
  transform_4[""TransformBlock&lt;Int32,String&gt;""]
  batch_3[""BatchBlock&lt;String&gt;""]
  batch_5[""BatchBlock&lt;String&gt;""]
  buffer_6[""BufferBlock&lt;String[]&gt;""]
  transformMany_7[""TransformManyBlock&lt;String[],String&gt;""]

  buffer_0 --> buffer_1
  buffer_1 --> transform_2
  buffer_1 --> transform_4
  transform_2 --> batch_3
  transform_4 --> batch_5
  batch_3 --> buffer_6
  batch_5 --> buffer_6
  buffer_6 --> transformMany_7
```
").SetName("dag builder should support longer fanouts");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .AddFanOutAndIn(new BufferBlock<int>(),
                [

                    new DataflowDAG<int>(nodeIdOffset: 1).AddFanOutAndIn(new BufferBlock<int>(),
                    [
                        new DataflowDAG<int>(nodeIdOffset: 2).Add(new TransformBlock<int, string>(i => $"{i}")),
                        new DataflowDAG<int>(nodeIdOffset: 3).Add(new TransformBlock<int, string>(i => $"{i}"))
                    ], new BufferBlock<string>()),
                    new DataflowDAG<int>(nodeIdOffset: 5).Add(new TransformBlock<int, string>(i => $"{i}")),
                ],
                new BufferBlock<string>()),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&lt;Int32&gt;""]
  buffer_1[""BufferBlock&lt;Int32&gt;""]
  transform_5[""TransformBlock&lt;Int32,String&gt;""]
  transform_2[""TransformBlock&lt;Int32,String&gt;""]
  transform_3[""TransformBlock&lt;Int32,String&gt;""]
  buffer_6[""BufferBlock&lt;String&gt;""]
  buffer_4[""BufferBlock&lt;String&gt;""]

  buffer_0 --> buffer_1
  buffer_0 --> transform_5
  buffer_1 --> transform_2
  buffer_1 --> transform_3
  transform_5 --> buffer_6
  transform_2 --> buffer_4
  transform_3 --> buffer_4
  buffer_4 --> buffer_6
```
").SetName("dag builder should support fanoutins within fanoutins and heterogeneous branches");

        yield return new TestCaseData(
            new DataflowDAG<int>()
                .Add(new ActionBlock<int>(i => { }))
                .Add(new BufferBlock<int>()),
@"```mermaid
graph TD
  action_0[""ActionBlock&lt;Int32&gt;""]
  buffer_1[""BufferBlock&lt;Int32&gt;""]

  action_0 --> buffer_1
```
").SetName("dag builder should support action block psuedo linking");

        // seperate node print stuff from dag structure??
    }
}