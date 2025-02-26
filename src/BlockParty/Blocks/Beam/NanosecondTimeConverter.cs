using System;

namespace BlockParty.Blocks.Beam;

public class NanosecondTimeConverter
{
    private static readonly DateTimeOffset _unixEpoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public long ConvertToNanosecondEpoch(DateTimeOffset dateTimeOffset)
    {
        var ticksSinceEpoch = dateTimeOffset.ToUniversalTime().Ticks - _unixEpoch.Ticks;
        return ticksSinceEpoch * 100;
    }

    public long ConvertToNanosecondEpoch(DateTime dateTime)
    {
        var ticksSinceEpoch = dateTime.ToUniversalTime().Ticks - _unixEpoch.Ticks;
        return ticksSinceEpoch * 100;
    }
}