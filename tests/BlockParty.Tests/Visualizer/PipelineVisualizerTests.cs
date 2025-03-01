using BlockParty.Visualizer;

namespace BlockParty.Tests.Visualizer;

public class PipelineVisualizerTests
{
    // todo delete
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

    [Theory]
    [TestCaseSource(nameof(PipelineVisualizerTestCases2))]
    public void VisualizeShouldGenerateExpectedMermaidGraph2(
        BlockNode2 blockLinkedList,
        string expectedVisualization)
    {
        // arrange
        var visualizer = new PipelineVisualizer();

        // act
        var visualization = visualizer.Visualize(blockLinkedList);

        // assert
        Assert.That(visualization, Is.EqualTo(expectedVisualization));
    }

    private static IEnumerable<TestCaseData> PipelineVisualizerTestCases2()
    {
        yield return new TestCaseData(
            BlockNode2.Create<int, int>("TransformBlock", 0,
            [BlockNode2.Create<int, int>("TransformBlock", 1)]),
@"```mermaid
graph TD
  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

  transform_0 --> transform_1
```
").SetName("2 should create a simple mermaid graph");

        yield return new TestCaseData(
            BlockNode2.Create<int, string>("TransformManyBlock", 0,
            [BlockNode2.Create<string, int>("TransformManyBlock", 1)]),
@"```mermaid
graph TD
  transformMany_0[""TransformManyBlock&nbsp;&lt;Int32,&nbsp;String&gt;""]
  transformMany_1[""TransformManyBlock&nbsp;&lt;String,&nbsp;Int32&gt;""]

  transformMany_0 --> transformMany_1
```
").SetName("2 should camel case names");

        yield return new TestCaseData(
            BlockNode2.Create<int, int>("FilterBlock", 0,
            [BlockNode2.Create<int, int[]>("TransformBlock", 1,
            [BlockNode2.Create<int[], int>("TransformManyBlock", 2,
            [BlockNode2.Create<int, string>("TransformBlock", 3,
            [BlockNode2.Create<string, string[]>("TransformManyBlock", 4,
            [BlockNode2.Create<string[], string>("TransformBlock", 5,
            [BlockNode2.Create<string, string>("FilterBlock", 6,
            [BlockNode2.Create<string, int>("TransformBlock", 7,
            [BlockNode2.Create<int, int>("FilterBlock", 8)])])])])])])])]),
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
").SetName("2 should visualize complicated transform manys");

        yield return new TestCaseData(
            BlockNode2.Create<int, TestClass>("ABlock", 0,
            [BlockNode2.Create<TestClass, TestTypedClass<int>>("BBlock", 1,
            [BlockNode2.Create<TestTypedClass<int>, SomeRecord>("CBlock", 2,
            [BlockNode2.Create<SomeRecord, SomeStruct>("DBlock", 3,
            [BlockNode2.Create<SomeStruct, MultiTypedClass<int, TestClass>>("EBlock", 4)])])])]),
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
").SetName("2 should support records and classes");

        yield return new TestCaseData(
            BlockNode2.Create<int, int>("TransformBlock", 0),
@"```mermaid
graph TD
  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

```
").SetName("2 should not bomb on 1 nodes");

        var downstreamBlock = BlockNode2.Create<int, int>("TransformBlock", 5);
        yield return new TestCaseData(
            BlockNode2.Create<int, int>("TransformBlock", 0,
            [
                BlockNode2.Create<int, int>("TransformBlock", 1,
                [BlockNode2.Create<int, int>("TransformBlock", 2, [downstreamBlock])]),

                BlockNode2.Create<int, int>("TransformBlock", 3,
                [BlockNode2.Create<int, int>("TransformBlock", 4, [downstreamBlock])])
            ]),
@"```mermaid
graph TD
  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_3[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_2[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_4[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
  transform_5[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

  transform_0 --> transform_1
  transform_0 --> transform_3
  transform_1 --> transform_2
  transform_3 --> transform_4
  transform_2 --> transform_5
  transform_4 --> transform_5
```
").SetName("2 should support fan out and joins");

        yield return new TestCaseData(
            BlockNode2.Create<int>("BufferBlock", 0,
            [BlockNode2.Create<long>("ActionBlock", 1)]),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&nbsp;&lt;Int32&gt;""]
  action_1[""ActionBlock&nbsp;&lt;Int64&gt;""]

  buffer_0 --> action_1
```
").SetName("2 should support single type blocks");

        yield return new TestCaseData(
            BlockNode2.Create<Dictionary<SomeRecord, List<int>>>("BufferBlock", 0),
@"```mermaid
graph TD
  buffer_0[""BufferBlock&nbsp;&lt;Dictionary&lt;SomeRecord,&nbsp;List&lt;Int32&gt;&gt;&gt;""]

```
").SetName("2 should interpolate nested generic types");
    }

    private static IEnumerable<TestCaseData> PipelineVisualizerTestCases()
    {
        return new List<TestCaseData>();
//        yield return new TestCaseData(
//            new BlockNode[]
//            {
//                BlockNode.Create<int, int>("TransformBlock"),
//                BlockNode.Create<int, int>("TransformBlock"),
//            },
//@"```mermaid
//graph TD
//  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
//  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

//  transform_0 --> transform_1
//```
//").SetName("should create a simple mermaid graph");

//        yield return new TestCaseData(
//            new BlockNode[]
//            {
//                BlockNode.Create<int, string>("TransformManyBlock"),
//                BlockNode.Create<string, int>("TransformManyBlock"),
//            },
//@"```mermaid
//graph TD
//  transformMany_0[""TransformManyBlock&nbsp;&lt;Int32,&nbsp;String&gt;""]
//  transformMany_1[""TransformManyBlock&nbsp;&lt;String,&nbsp;Int32&gt;""]

//  transformMany_0 --> transformMany_1
//```
//").SetName("should camel case names");

//        yield return new TestCaseData(
//            new BlockNode[]
//            {
//                BlockNode.Create<int, int>("FilterBlock"),
//                BlockNode.Create<int, int[]>("TransformBlock"),
//                BlockNode.Create<int[], int>("TransformManyBlock"),
//                BlockNode.Create<int, string>("TransformBlock"),
//                BlockNode.Create<string, string[]>("TransformManyBlock"),
//                BlockNode.Create<string[], string>("TransformBlock"),
//                BlockNode.Create<string, string>("FilterBlock"),
//                BlockNode.Create<string, int>("TransformBlock"),
//                BlockNode.Create<int, int>("FilterBlock"),
//            },
//@"```mermaid
//graph TD
//  filter_0[""FilterBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]
//  transform_1[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32[]&gt;""]
//  transformMany_2[""TransformManyBlock&nbsp;&lt;Int32[],&nbsp;Int32&gt;""]
//  transform_3[""TransformBlock&nbsp;&lt;Int32,&nbsp;String&gt;""]
//  transformMany_4[""TransformManyBlock&nbsp;&lt;String,&nbsp;String[]&gt;""]
//  transform_5[""TransformBlock&nbsp;&lt;String[],&nbsp;String&gt;""]
//  filter_6[""FilterBlock&nbsp;&lt;String,&nbsp;String&gt;""]
//  transform_7[""TransformBlock&nbsp;&lt;String,&nbsp;Int32&gt;""]
//  filter_8[""FilterBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

//  filter_0 --> transform_1
//  transform_1 --> transformMany_2
//  transformMany_2 --> transform_3
//  transform_3 --> transformMany_4
//  transformMany_4 --> transform_5
//  transform_5 --> filter_6
//  filter_6 --> transform_7
//  transform_7 --> filter_8
//```
//").SetName("should visualize complicated transform manys");

//        yield return new TestCaseData(
//            new BlockNode[]
//            {
//                BlockNode.Create<int, TestClass>("ABlock"),
//                BlockNode.Create<TestClass, TestTypedClass<int>>("BBlock"),
//                BlockNode.Create<TestTypedClass<int>, SomeRecord>("CBlock"),
//                BlockNode.Create<SomeRecord, SomeStruct>("DBlock"),
//                BlockNode.Create<SomeStruct, MultiTypedClass<int, TestClass>>("EBlock"),
//            },
//@"```mermaid
//graph TD
//  a_0[""ABlock&nbsp;&lt;Int32,&nbsp;TestClass&gt;""]
//  b_1[""BBlock&nbsp;&lt;TestClass,&nbsp;TestTypedClass&lt;Int32&gt;&gt;""]
//  c_2[""CBlock&nbsp;&lt;TestTypedClass&lt;Int32&gt;,&nbsp;SomeRecord&gt;""]
//  d_3[""DBlock&nbsp;&lt;SomeRecord,&nbsp;SomeStruct&gt;""]
//  e_4[""EBlock&nbsp;&lt;SomeStruct,&nbsp;MultiTypedClass&lt;Int32,&nbsp;TestClass&gt;&gt;""]

//  a_0 --> b_1
//  b_1 --> c_2
//  c_2 --> d_3
//  d_3 --> e_4
//```
//").SetName("should support records and classes");

//        yield return new TestCaseData(
//            new BlockNode[] { },
//@"```mermaid
//graph TD

//```
//").SetName("should not bomb on 0 nodes");

//        yield return new TestCaseData(
//            new BlockNode[] 
//            {
//                BlockNode.Create<int, int>("TransformBlock"),
//            },
//@"```mermaid
//graph TD
//  transform_0[""TransformBlock&nbsp;&lt;Int32,&nbsp;Int32&gt;""]

//```
//").SetName("should not bomb on 1 nodes");
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