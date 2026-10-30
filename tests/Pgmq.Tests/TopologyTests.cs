namespace Pgmq.Tests;

public sealed class TopologyTests
{
    [Fact]
    public void A_message_Should_go_to_every_queue_subscribed_to_its_type_or_a_base_type()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(OrderCreated), "billing");
        topology.Subscribe(typeof(OrderCreated), "shipping");
        topology.Subscribe(typeof(OrderEvent), "audit");
        topology.Subscribe(typeof(Unrelated), "elsewhere");

        Assert.Equal(["billing", "shipping", "audit"], topology.QueuesFor(typeof(OrderCreated)));
        Assert.Equal(["audit"], topology.QueuesFor(typeof(OrderCancelled)));
        Assert.Empty(topology.QueuesFor(typeof(string)));
    }

    [Fact]
    public void A_later_subscription_Should_change_where_a_type_goes()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(OrderCreated), "billing");
        Assert.Single(topology.QueuesFor(typeof(OrderCreated)));

        topology.Subscribe(typeof(OrderCreated), "shipping");

        Assert.Equal(2, topology.QueuesFor(typeof(OrderCreated)).Count);
    }

    [Fact]
    public void A_subscribed_type_or_a_type_deriving_from_one_Should_resolve_by_name()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(OrderEvent), "audit");

        Assert.Equal(typeof(OrderEvent), topology.ResolveMessageType(typeof(OrderEvent).FullName!));
        Assert.Equal(typeof(OrderCancelled), topology.ResolveMessageType(typeof(OrderCancelled).FullName!));
    }

    [Fact]
    public void A_name_unrelated_to_any_subscription_Should_not_resolve()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(OrderEvent), "audit");

        Assert.Null(topology.ResolveMessageType(typeof(Unrelated).FullName!));
        Assert.Null(topology.ResolveMessageType("System.IO.FileInfo"));
        Assert.Null(topology.ResolveMessageType("no such type"));
    }

    [Fact]
    public void A_pinned_message_name_Should_be_what_travels_and_what_resolves()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(Renamed), "renamed");

        Assert.Equal(typeof(Renamed), topology.ResolveMessageType("orders.renamed.v1"));
        Assert.Null(topology.ResolveMessageType(typeof(Renamed).FullName!));
    }

    [Fact]
    public void Two_types_travelling_under_one_name_Should_be_refused()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(Renamed), "renamed");

        Assert.Throws<InvalidOperationException>(() => topology.Subscribe(typeof(SameName), "other"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("Upper")]
    [InlineData("with-dash")]
    [InlineData("with space")]
    [InlineData("a_name_that_is_far_too_long_for_a_pgmq_queue_name_x")]
    public void An_invalid_queue_name_Should_be_refused(string name)
        => Assert.Throws<ArgumentException>(() => new PgmqTopology().Subscribe(typeof(OrderCreated), name));

    [Fact]
    public void Queue_names_Should_list_each_queue_once()
    {
        PgmqTopology topology = new();
        topology.Subscribe(typeof(OrderCreated), "billing");
        topology.Subscribe(typeof(OrderCancelled), "billing");

        Assert.Equal(["billing"], topology.QueueNames);
    }

    public abstract record OrderEvent;

    public sealed record OrderCreated : OrderEvent;

    public sealed record OrderCancelled : OrderEvent;

    public sealed record Unrelated;

    [MessageName("orders.renamed.v1")]
    public sealed record Renamed;

    [MessageName("orders.renamed.v1")]
    public sealed record SameName;
}
