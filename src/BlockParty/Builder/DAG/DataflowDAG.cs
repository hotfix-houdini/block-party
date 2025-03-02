using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace BlockParty.Builder.DAG;


// suupport building into an IPropagatorBlock (encapsulate)
// AND support building into an ITArgetBlock (ending with an action block and making my own encapsulate implementation) 

public class DataflowDAG<TInput> : DataflowDAG<TInput, TInput>
{
    public DataflowDAG(int nodeIdOffset = 0) : base(nodeIdOffset, 0)
    {

    }
}
public class DataflowDAG<TInput, TOutput>
{
    public int _nodeId;
    public int _nodeCount;
    public PropagatorNode HeadNode { get; private set; }
    public PropagatorNode TailNode { get; private set; } // potentially a TargetNode too? I.e. a generic node (for actionblock ending) 

    public DataflowDAG(int nodeIdOffset = 0) : this(nodeIdOffset, 0) { }

    protected DataflowDAG(int nodeIdOffset, int totalNodeCount)
    {
        _nodeId = nodeIdOffset;
        _nodeCount = totalNodeCount;
    }

    public DataflowDAG<TInput, TNewType> Add<TNewType>(IPropagatorBlock<TOutput, TNewType> nextNode)
    {
        var newNode = PropagatorNode.Create<TOutput, TNewType>(nextNode, _nodeId);
        if (TailNode != null)
        {
            TailNode.Children.Add(newNode);
        }

        return new DataflowDAG<TInput, TNewType>(++_nodeId, ++_nodeCount)
        {
            HeadNode = this.HeadNode ?? newNode,
            TailNode = newNode
        };
    }

    public DataflowDAG<TInput, TOutput> Add(ITargetBlock<TOutput> nextNode)
    {
        var newNode = PropagatorNode.Create<TOutput, TOutput>(nextNode, _nodeId);
        if (TailNode != null)
        {
            TailNode.Children.Add(newNode);
        }

        return new DataflowDAG<TInput, TOutput>(++_nodeId, ++_nodeCount)
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
        var fanoutNode = PropagatorNode.Create<TOutput, TOutput>(fanoutBlock, _nodeId++);
        _nodeCount++;
        if (TailNode != null)
        {
            TailNode.Children.Add(fanoutNode);
        }
        foreach (var fanoutDag in fanOutDags)
        {
            fanoutNode.Children.Add(fanoutDag.HeadNode);
            _nodeCount += fanoutDag._nodeCount;
            _nodeId += fanoutDag._nodeCount;
        }

        var fanInNode = PropagatorNode.Create<TOutput, TOutput>(fanInBlock, _nodeId++);
        _nodeCount++;
        foreach (var fanoutDag in fanOutDags)
        {
            fanoutDag.TailNode.Children.Add(fanInNode);
        }

        return new DataflowDAG<TInput, TNewType>(_nodeId, _nodeCount)
        {
            HeadNode = this.HeadNode ?? fanoutNode,
            TailNode = fanInNode
        };
    }

    public string GenerateMermaidGraph()
    {
        var mermaidNodes = new StringBuilder();
        var mermaidLinks = new StringBuilder();

        var processedNodes = new HashSet<PropagatorNode>();
        var nodeQueue = new Queue<PropagatorNode>();
        nodeQueue.Enqueue(HeadNode);
        processedNodes.Add(HeadNode);

        while (nodeQueue.Count > 0)
        {
            var node = nodeQueue.Dequeue();
            mermaidNodes.AppendLine($"  {IndexedNodeName(node)}[\"{ConstructTypeName(node.Block.GetType())}\"]");


            foreach (var childNode in node.Children)
            {
                mermaidLinks.AppendLine($"  {IndexedNodeName(node)} --> {IndexedNodeName(childNode)}");
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

    private static string IndexedNodeName(PropagatorNode node)
    {
        return $"{ToCamelCase(node.Name()).Replace("Block", "")}_{node.Id}";
    }

    private static string ToCamelCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        var result = new StringBuilder(str);
        result[0] = char.ToLower(result[0]);

        return result.ToString();
    }

    private static string ConstructTypeName(Type type)
    {
        if (!type.IsGenericType)
            return type.Name;

        var typeName = type.Name.Substring(0, type.Name.IndexOf('`'));
        var typeArguments = type.GetGenericArguments()
                                .Select(ConstructTypeName); // Recursively process generic arguments

        return $"{typeName}&lt;{string.Join(",", typeArguments)}&gt;";
    }

    // target (head)
    // iternal flow
    // source (tail) (potentially support an action block to?) 

    // add node.

    // make the DAG the source of truth?!?!
    // RENDER the dag for mermaid instead of my string maps
    // down the road could use the dag for instrumentation (tracks each block). I.e. wrap each block with an aggregate block or something.
    // mermaid settings - with types? with "Block" name? remove the verbosity.... 
}