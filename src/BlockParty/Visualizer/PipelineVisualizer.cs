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

    private static string NodeName(BlockNode node, int i)
    {
        return $"{ToCamelCase(node.Name).Replace("Block", "")}_{i}";
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