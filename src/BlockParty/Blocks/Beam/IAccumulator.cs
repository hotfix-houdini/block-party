namespace BlockParty.Blocks.Beam;

public interface IAccumulator
{
    long WindowStart { get; set; }
    long WindowEnd { get; set; }
}