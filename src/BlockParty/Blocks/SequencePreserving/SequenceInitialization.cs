namespace BlockParty.Blocks.SequencePreserving;

public readonly struct SequenceInitialization
{
    private readonly long _inclusiveStartingSequenceNumber;
    private readonly bool _fromFirstElement;

    public long InclusiveStartingSequenceNumber => _inclusiveStartingSequenceNumber;
    public bool FromFirstElementFlag => _fromFirstElement;

    private SequenceInitialization(long value, bool fromFirstElement)
    {
        _inclusiveStartingSequenceNumber = value;
        _fromFirstElement = fromFirstElement;
    }

    /// <summary>
    /// Sets the sequence number based on the first element that arrives in the block.<br/>
    /// Example: [3, 1, 2] => [3]. 3 will be selected as the initial sequence number, and 1 and 2 will be discarded.
    /// </summary>
    public static SequenceInitialization FromFirstElement => new(0, true);

    /// <summary>
    /// Allows you to specify the initial sequence number with a known sequence, like if you have a "last processed index" in a database somewhere.<br/>
    /// Example: [3, 1, 2] with initial sequence number of 1 => [1, 2, 3]. 1 will be set as the initial sequence number and the block will successfully reorder the stream.
    /// </summary>
    public static SequenceInitialization From(long inclusiveStartingSequenceNumber) => new(inclusiveStartingSequenceNumber, false);
}
