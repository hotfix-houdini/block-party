using BlockParty.Blocks.Beam;

namespace BlockParty.Tests.Blocks;

public class NanosecondEpochTimeConverterTests
{
    [Theory]
    [TestCase(2024, 03, 28, 07, 03, 0, 0, -5, 1711627380_000000000)]
    [TestCase(2024, 03, 28, 07, 03, 0, 0, 0, 1711609380_000000000)]
    [TestCase(2024, 02, 29, 12, 52, 32, 0, -6, 1709232752_000000000)]
    [TestCase(2024, 02, 29, 12, 52, 32, 1234567, -6, 1709232752_123456700)]
    public void ShouldConvertDateTimeOffsets(
        int year,
        int month,
        int day,
        int hour,
        int minute,
        int second,
        int hundredNanoseconds,
        int timezoneOffset,
        long expectedNanosecondEpoch)
    {
        // arrange
        var dateTimeOffset = new DateTimeOffset(year, month, day, hour, minute, second, TimeSpan.FromHours(timezoneOffset)).AddTicks(hundredNanoseconds);
        var converter = new NanosecondTimeConverter();

        // act
        var actualNanosecondEpoch = converter.ConvertToNanosecondEpoch(dateTimeOffset);

        // assert
        Assert.That(actualNanosecondEpoch, Is.EqualTo(expectedNanosecondEpoch));
    }

    [Theory]
    [TestCase("2024-03-28T07:03:00-05:00", 1711627380_000000000)]
    [TestCase("2024-03-28T07:03:00Z", 1711609380_000000000)]
    [TestCase("2024-03-28T07:03:12.3456789Z", 1711609392_345678900)]
    [TestCase("1999-12-31T23:59:59Z", 946684799_000000000)]
    public void ShouldConvertDateTimes(string dateTimeString, long expectedNanosecondEpoch)
    {
        // arrange
        var dateTime = DateTime.Parse(dateTimeString);
        var converter = new NanosecondTimeConverter();

        // act
        var actualNanosecondEpoch = converter.ConvertToNanosecondEpoch(dateTime);

        // assert
        Assert.That(actualNanosecondEpoch, Is.EqualTo(expectedNanosecondEpoch));
    }

    [Theory]
    [TestCase(946684799)]
    [TestCase(1711627380)]
    [TestCase(220924799)]
    public void ShouldMatchRoundTripConversion(long epochTimeStampSeconds)
    {
        // arrange
        var dateTime = DateTimeOffset.FromUnixTimeSeconds(epochTimeStampSeconds);
        var converter = new NanosecondTimeConverter();

        // act
        var actualNanosecondEpoch = converter.ConvertToNanosecondEpoch(dateTime);

        // assert
        Assert.That(actualNanosecondEpoch, Is.EqualTo(epochTimeStampSeconds * 1_000_000_000));
    }
}
