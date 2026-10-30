using System.Diagnostics;

namespace Pgmq.Tests;

public sealed class MessageHeadersTests
{
    [Fact]
    public void Headers_Should_read_back_as_written()
    {
        MessageHeaders written = new("orders.created", Guid.CreateVersion7(), 3,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "vendor=value");

        MessageHeaders? read = MessageHeaders.From(written.ToDictionary());

        Assert.Equal(written, read);
    }

    [Fact]
    public void A_group_and_custom_headers_Should_read_back_as_written()
    {
        MessageHeaders written = MessageHeaders.New(typeof(Pinned), groupId: "g1",
            custom: new Dictionary<string, string> { ["tenant"] = "acme" });

        MessageHeaders? read = MessageHeaders.From(written.ToDictionary());

        Assert.Equal("g1", read?.GroupId);
        Assert.Equal("acme", read?.CustomHeaders["tenant"]);
        Assert.Single(read!.CustomHeaders);
    }

    [Fact]
    public void Headers_without_a_message_type_Should_not_read()
    {
        Assert.Null(MessageHeaders.From(null));
        Assert.Null(MessageHeaders.From(new Dictionary<string, object> { ["message-id"] = Guid.NewGuid().ToString() }));
    }

    [Fact]
    public void New_headers_Should_carry_the_current_trace()
    {
        using Activity activity = new Activity("publishing").SetIdFormat(ActivityIdFormat.W3C).Start();

        MessageHeaders headers = MessageHeaders.New(typeof(Pinned));

        Assert.Equal(activity.Id, headers.TraceParent);
        Assert.Equal("pinned", headers.MessageType);
        Assert.NotNull(headers.MessageId);
        Assert.Equal(0, headers.RedeliveryCount);
    }

    [Fact]
    public void A_message_name_Should_be_its_full_name_unless_pinned()
    {
        Assert.Equal(typeof(Unpinned).FullName, MessageNames.Of(typeof(Unpinned)));
        Assert.Equal("pinned", MessageNames.Of(typeof(Pinned)));
    }

    [Fact]
    public void A_message_Should_round_trip_through_the_serializer()
    {
        Unpinned message = new(42, "text", Kind.Second);

        object back = MessageSerializer.Deserialize(MessageSerializer.Serialize(message, typeof(Unpinned)), typeof(Unpinned));

        Assert.Equal(message, back);
    }

    [MessageName("pinned")]
    public sealed record Pinned;

    public sealed record Unpinned(int Number, string Text, Kind Kind);

    public enum Kind
    {
        First,
        Second,
    }
}
