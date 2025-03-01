using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BlockParty.Visualizer;

public class PipelineVisualizer
{
    public string Visualize(IEnumerable<BlockNode> blocks)
    {
        var mermaidNodes = new StringBuilder();
        var mermaidLinks = new StringBuilder();

        var indexedBlocks = blocks.Select((b, i) => (b, i)).ToList();
        foreach (var (block, index) in indexedBlocks)
        {
            mermaidNodes.AppendLine($"  {NodeName(block, index)}[\"{block.Name}&nbsp;&lt;{block.InputType},&nbsp;{block.OutputType}&gt;\"]");
            if (index < indexedBlocks.Count - 1)
            {
                var nextBlock = indexedBlocks[index + 1];
                mermaidLinks.AppendLine($"  {NodeName(block, index)} --> {NodeName(nextBlock.b, nextBlock.i)}");
            }
        }

        return $@"```mermaid
graph TD
{mermaidNodes}
{mermaidLinks}```
";
    }

    public string Visualize(BlockNode2 blockList)
    {
        var mermaidNodes = new StringBuilder();
        var mermaidLinks = new StringBuilder();

        var processedNodes = new HashSet<int>();
        var nodeQueue = new Queue<BlockNode2>();
        nodeQueue.Enqueue(blockList);
        processedNodes.Add(blockList.Id);

        while (nodeQueue.Count > 0)
        {
            var node = nodeQueue.Dequeue();
            mermaidNodes.AppendLine($"  {NodeName(node)}[\"{node.Name}&nbsp;&lt;{NodeTypes(node)}&gt;\"]");

            foreach (var linkedChild in node.Children)
            {
                mermaidLinks.AppendLine($"  {NodeName(node)} --> {NodeName(linkedChild)}");
                if (!processedNodes.Contains(linkedChild.Id))
                {
                    nodeQueue.Enqueue(linkedChild);
                    processedNodes.Add(linkedChild.Id);
                }
            }
        }

        return $@"```mermaid
graph TD
{mermaidNodes}
{mermaidLinks}```
";
    }

    private static string NodeName(BlockNode node, int i)
    {
        return $"{ToCamelCase(node.Name).Replace("Block", "")}_{i}";
    }

    private static string NodeName(BlockNode2 node)
    {
        return $"{ToCamelCase(node.Name).Replace("Block", "")}_{node.Id}";
    }

    private static string NodeTypes(BlockNode2 node)
    {
        if (node.OutputType == null)
        {
            return $"{node.InputType}";
        }
        else
        {
            return $"{node.InputType},&nbsp;{node.OutputType}";
        }
    }

    private static string ToCamelCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        var result = new StringBuilder(str);
        result[0] = char.ToLower(result[0]);

        return result.ToString();
    }
}