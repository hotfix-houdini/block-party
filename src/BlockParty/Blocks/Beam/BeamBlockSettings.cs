namespace BlockParty.Blocks.Beam
{
    public class BeamBlockSettings
    {
        /// <summary>
        /// When BeamBlock is completed it might have a partial window accumulated.<br/>
        /// This property, when FALSE, will emit that final - potentially partial - window on completition.<br/>
        /// When TRUE, the final - potentially partial - window will NOT be POSTed to downstream blocks and will be discarded.
        /// </summary>
        public bool OmitIncompleteFinalWindow { get; set; }
    }
}
