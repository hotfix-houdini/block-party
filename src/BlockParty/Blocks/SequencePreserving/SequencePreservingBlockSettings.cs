namespace BlockParty.Blocks.SequencePreserving;

public class SequencePreservingBlockSettings
{
    public SequenceInitialization SequenceInitialization { get; set; } = SequenceInitialization.FromFirstElement;
    public OnCompleteBufferedMessageBehavior OnCompleteBufferedMessageBehavior { get; set; } = OnCompleteBufferedMessageBehavior.Discard;
}
