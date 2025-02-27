using BlockParty.Visualizer;

namespace BlockParty.Tests.Visualizer;

public class PipelineVisualizerTests
{
    [Theory]
    [TestCaseSource(nameof(PipelineVisualizerTestCases))]
    public void VisualizeShouldGenerateExpectedMermaidGraph(
        BlockNode[] pipeline,
        string expectedVisualization)
    {
        // arrange
        var visualizer = new PipelineVisualizer();

        // act
        var visualization = visualizer.Visualize(pipeline);

        // assert
        Assert.That(visualization, Is.EqualTo(expectedVisualization));
    }

    private static IEnumerable<TestCaseData> PipelineVisualizerTestCases()
    {
        yield return new TestCaseData(
            new BlockNode[]
            {
                BlockNode.Create<int, int>("TransformBlock"),
                BlockNode.Create<int, int>("TransformBlock"),
            },
@"```mermaid
graph TD
  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

  transform_0 --> transform_1
```
").SetName("should create a simple mermaid graph");

        yield return new TestCaseData(
            new BlockNode[]
            {
                BlockNode.Create<int, string>("TransformManyBlock"),
                BlockNode.Create<string, int>("TransformManyBlock"),
            },
@"```mermaid
graph TD
  transformMany_0[""TransformManyBlock&nbsp;&lt;Int32,&nbsp;String&gt;""]
  transformMany_1[""TransformManyBlock&nbsp;&lt;String,&nbsp;Int32&gt;""]

  transformMany_0 --> transformMany_1
```
").SetName("should camel case names");

        yield return new TestCaseData(
            new BlockNode[]
            {
                BlockNode.Create<int, int>("FilterBlock"),
                BlockNode.Create<int, int[]>("TransformBlock"),
                BlockNode.Create<int[], int>("TransformManyBlock"),
                BlockNode.Create<int, string>("TransformBlock"),
                BlockNode.Create<string, string[]>("TransformManyBlock"),
                BlockNode.Create<string[], string>("TransformBlock"),
                BlockNode.Create<string, string>("FilterBlock"),
                BlockNode.Create<string, int>("TransformBlock"),
                BlockNode.Create<int, int>("FilterBlock"),
            },
@"```mermaid
graph TD
  filter_0[""FilterBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32[]&gt;""]
  transformMany_2[""TransformManyBlock&nbsp;&lt;Int32[],&nbsp;Int32&gt;""]
  transform_3[""TransformBlock&nbsp;&lt;Int32,&nbsp;String&gt;""]
  transformMany_4[""TransformManyBlock&nbsp;&lt;String,&nbsp;String[]&gt;""]
  transform_5[""TransformBlock&nbsp;&lt;String[],&nbsp;String&gt;""]
  filter_6[""FilterBlock&nbsp;&lt;String,&nbsp;String&gt;""]
  transform_7[""TransformBlock&nbsp;&lt;String,&nbsp;Int32&gt;""]
  filter_8[""FilterBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

  filter_0 --> transform_1
  transform_1 --> transformMany_2
  transformMany_2 --> transform_3
  transform_3 --> transformMany_4
  transformMany_4 --> transform_5
  transform_5 --> filter_6
  filter_6 --> transform_7
  transform_7 --> filter_8
```
").SetName("should visualize complicated transform manys");

        yield return new TestCaseData(
            new BlockNode[]
            {
                BlockNode.Create<int, TestClass>("ABlock"),
                BlockNode.Create<TestClass, TestTypedClass<int>>("BBlock"),
                BlockNode.Create<TestTypedClass<int>, SomeRecord>("CBlock"),
                BlockNode.Create<SomeRecord, SomeStruct>("DBlock"),
                BlockNode.Create<SomeStruct, MultiTypedClass<int, TestClass>>("EBlock"),
            },
@"```mermaid
graph TD
  a_0[""ABlock&nbsp;&lt;Int32,&nbsp;TestClass&gt;""]
  b_1[""BBlock&nbsp;&lt;TestClass,&nbsp;TestTypedClass&lt;Int32&gt;&gt;""]
  c_2[""CBlock&nbsp;&lt;TestTypedClass&lt;Int32&gt;,&nbsp;SomeRecord&gt;""]
  d_3[""DBlock&nbsp;&lt;SomeRecord,&nbsp;SomeStruct&gt;""]
  e_4[""EBlock&nbsp;&lt;SomeStruct,&nbsp;MultiTypedClass&lt;Int32,&nbsp;TestClass&gt;&gt;""]

  a_0 --> b_1
  b_1 --> c_2
  c_2 --> d_3
  d_3 --> e_4
```
").SetName("should support records and classes");

        yield return new TestCaseData(
            new BlockNode[] { },
@"```mermaid
graph TD

```
").SetName("should not bomb on 0 nodes");

        yield return new TestCaseData(
            new BlockNode[] 
            {
                BlockNode.Create<int, int>("TransformBlock"),
            },
@"```mermaid
graph TD
  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

```
").SetName("should not bomb on 1 nodes");
    }
}

class TestClass
{

}

class TestTypedClass<TType>
{

}

record SomeRecord
{

}

struct SomeStruct
{

}

class MultiTypedClass<TType1, TType2>
{

}