namespace BlockParty.Blocks.SequencePreserving;

public enum OnCompleteBufferedMessageBehavior
{
    /// <summary>
    /// Example: [1, 2, 4] => [1, 2]. 4 is non-contiguous and discarded on block complete. Default behavior.
    /// </summary>
    Discard = 0,

    /// <summary>
    /// Example: [1, 2, 4] => [1, 2, 4]. 4 is non-contiguous but this option emits 4 on complete despite being non-contiguous.
    /// </summary>
    Emit = 1,        
}
