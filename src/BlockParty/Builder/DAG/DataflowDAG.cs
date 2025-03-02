using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;

public class DataflowDAG<TInput> : DataflowDAG<TInput, TInput>
{
    public DataflowDAG(int nodeIdOffset = 0) : base(nodeIdOffset, 0)
    {
    }
}

public class DataflowDAG<TInput, TOutput>
{
    public int NextNodeId { get; private set; }
    public int NodeCount { get; private set; }
    public DAGNode HeadNode { get; private set; }
    public DAGNode TailNode { get; private set; }

    public DataflowDAG(int nodeIdOffset = 0) : this(nodeIdOffset, 0) { }

    protected DataflowDAG(int nodeIdOffset, int totalNodeCount)
    {
        NextNodeId = nodeIdOffset;
        NodeCount = totalNodeCount;
    }

    public DataflowDAG<TInput, TNewType> Add<TNewType>(IPropagatorBlock<TOutput, TNewType> nextNode)
    {
        var newNode = DAGNode.Create<TOutput, TNewType>(nextNode, NextNodeId);
        if (TailNode != null)
        {
            TailNode.Children.Add(newNode);
        }

        return new DataflowDAG<TInput, TNewType>(++NextNodeId, ++NodeCount)
        {
            HeadNode = this.HeadNode ?? newNode,
            TailNode = newNode
        };
    }

    public DataflowDAG<TInput, TOutput> Add(ITargetBlock<TOutput> nextNode)
    {
        var newNode = DAGNode.Create<TOutput, TOutput>(nextNode, NextNodeId);
        if (TailNode != null)
        {
            TailNode.Children.Add(newNode);
        }

        return new DataflowDAG<TInput, TOutput>(++NextNodeId, ++NodeCount)
        {
            HeadNode = this.HeadNode ?? newNode,
            TailNode = newNode
        };
    }

    public DataflowDAG<TInput, TNewType> AddFanOutAndIn<TNewType>(
        ITargetBlock<TOutput> fanoutBlock,
        IEnumerable<DataflowDAG<TOutput, TNewType>> fanOutDags,
        IPropagatorBlock<TNewType, TNewType> fanInBlock)
    {
        var fanoutNode = DAGNode.Create<TOutput, TOutput>(fanoutBlock, NextNodeId++);
        NodeCount++;
        if (TailNode != null)
        {
            TailNode.Children.Add(fanoutNode);
        }
        foreach (var fanoutDag in fanOutDags)
        {
            fanoutNode.Children.Add(fanoutDag.HeadNode);
            NodeCount += fanoutDag.NodeCount;
            NextNodeId += fanoutDag.NodeCount;
        }

        var fanInNode = DAGNode.Create<TOutput, TOutput>(fanInBlock, NextNodeId++);
        NodeCount++;
        foreach (var fanoutDag in fanOutDags)
        {
            fanoutDag.TailNode.Children.Add(fanInNode);
        }

        return new DataflowDAG<TInput, TNewType>(NextNodeId, NodeCount)
        {
            HeadNode = this.HeadNode ?? fanoutNode,
            TailNode = fanInNode
        };
    }

    public string GenerateMermaidGraph()
    {
        var mermaidNodes = new StringBuilder();
        var mermaidLinks = new StringBuilder();

        var processedNodes = new HashSet<DAGNode>();
        var nodeQueue = new Queue<DAGNode>();
        nodeQueue.Enqueue(HeadNode);
        processedNodes.Add(HeadNode);

        while (nodeQueue.Count > 0)
        {
            var node = nodeQueue.Dequeue();
            mermaidNodes.AppendLine($"  {node.IndexedNodeName()}[\"{node.ConstructTypeName()}\"]");


            foreach (var childNode in node.Children)
            {
                mermaidLinks.AppendLine($"  {node.IndexedNodeName()} --> {childNode.IndexedNodeName()}");
                if (!processedNodes.Contains(childNode))
                {
                    nodeQueue.Enqueue(childNode);
                    processedNodes.Add(childNode);
                }
            }
        }

        return $@"```mermaid
graph TD
{mermaidNodes}
{mermaidLinks}```
";
    }
}