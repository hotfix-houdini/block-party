namespace BlockParty.Blocks.SequencePreserving
{
    public struct SequenceInitialization
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

        public static SequenceInitialization FromFirstElement => new SequenceInitialization(0, true);
        public static SequenceInitialization From(long inclusiveStartingSequenceNumber) => new SequenceInitialization(inclusiveStartingSequenceNumber, false);
    }
}
