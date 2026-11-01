namespace Pgmq.Examples.WebApi;

// Every consumer here writes what it did to the ActivityFeed, so GET /activity shows the flow without reading logs.
// [QueueName] pins each consumer's queue name, so the class can be renamed later.

/// <summary>
/// Handles a new order and asks for its payment. What a consumer publishes through its context goes out only if the
/// consumer succeeds (an outbox), so a retried order never asks for payment twice.
/// </summary>
[QueueName("example_order_placed")]
public sealed class OrderPlacedHandler(ActivityFeed feed) : IPgmqConsumer<OrderPlaced>
{
    public async Task HandleAsync(PgmqConsumeContext<OrderPlaced> context, CancellationToken cancellationToken)
    {
        OrderPlaced order = context.Message;
        feed.Add(nameof(OrderPlacedHandler), $"Order {order.OrderId} from {order.Customer}, total {order.Total}");

        await context.PublishAsync(new PaymentRequested(order.OrderId, order.Total, order.PaymentFailures),
            cancellationToken);
        await context.PublishToTopicAsync($"notify.email.{RoutingWord(order.Customer)}",
            new Notification($"Thanks for your order, {order.Customer}!"), cancellationToken);
    }

    /// <summary>A routing key word allows only letters, digits, '-' and '_': "Big Spender" becomes "big-spender".</summary>
    private static string RoutingWord(string text)
        => string.Concat(text.Trim().ToLowerInvariant().Select(c => char.IsAsciiLetterOrDigit(c) || c == '_' ? c : '-'));
}

/// <summary>
/// Charges an order through a fake, flaky gateway. Shows the two kinds of failure:
/// a transient one (here a <see cref="TimeoutException"/>) is retried with backoff, up to <c>MaxAttempts</c>;
/// any other exception archives the message to <c>pgmq.a_example_payments</c> straight away.
/// </summary>
[QueueName("example_payments")]
[ConsumerOptions(Concurrency = 2, TimeoutSeconds = 30)]
public sealed class PaymentHandler(ActivityFeed feed) : IPgmqConsumer<PaymentRequested>
{
    public async Task HandleAsync(PgmqConsumeContext<PaymentRequested> context, CancellationToken cancellationToken)
    {
        PaymentRequested payment = context.Message;
        int attempt = context.RedeliveryCount + 1;

        await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken); // talking to the "gateway"

        if (payment.Amount > 10_000)
        {
            feed.Add(nameof(PaymentHandler), $"Order {payment.OrderId}: amount {payment.Amount} refused, archived");
            throw new InvalidOperationException($"Amount {payment.Amount} is over the limit."); // permanent
        }

        if (attempt <= payment.FailTimes)
        {
            feed.Add(nameof(PaymentHandler), $"Order {payment.OrderId}: gateway timed out (attempt {attempt})");
            throw new TimeoutException("The payment gateway timed out."); // transient: retried
        }

        feed.Add(nameof(PaymentHandler), $"Order {payment.OrderId}: charged {payment.Amount} (attempt {attempt})");
    }
}

/// <summary>
/// Gets every <see cref="Notification"/> published by type, plus topic publishes to <c>notify.email.*</c>.
/// <c>*</c> matches exactly one word.
/// </summary>
[QueueName("example_email")]
[TopicBinding("notify.email.*")]
public sealed class EmailNotifier(ActivityFeed feed) : IPgmqConsumer<Notification>
{
    public Task HandleAsync(PgmqConsumeContext<Notification> context, CancellationToken cancellationToken)
    {
        feed.Add(nameof(EmailNotifier), $"Email: {context.Message.Text}");
        return Task.CompletedTask;
    }
}

/// <summary>
/// Gets every <see cref="Notification"/> published by type, plus topic publishes to anything under <c>notify.</c>.
/// <c>#</c> matches any number of words.
/// </summary>
[QueueName("example_audit")]
[TopicBinding("notify.#")]
public sealed class NotificationAudit(ActivityFeed feed) : IPgmqConsumer<Notification>
{
    public Task HandleAsync(PgmqConsumeContext<Notification> context, CancellationToken cancellationToken)
    {
        string via = context.Headers.TryGetValue("x-source", out string? source) ? $" (from {source})" : "";
        feed.Add(nameof(NotificationAudit), $"Audit: {context.Message.Text}{via}");
        return Task.CompletedTask;
    }
}

/// <summary>
/// FIFO groups: entries of one account (the group) are handled one at a time, in the order they were published.
/// Different accounts run in parallel, up to <c>Concurrency</c>.
/// </summary>
[QueueName("example_ledger")]
[ConsumerOptions(ReadMode = PgmqReadMode.Grouped, Concurrency = 4)]
public sealed class LedgerHandler(ActivityFeed feed) : IPgmqConsumer<LedgerEntry>
{
    public async Task HandleAsync(PgmqConsumeContext<LedgerEntry> context, CancellationToken cancellationToken)
    {
        LedgerEntry entry = context.Message;
        await Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(50, 300)), cancellationToken);
        feed.Add(nameof(LedgerHandler), $"Account {context.GroupId}: entry #{entry.Sequence}, {entry.Amount:+0.00;-0.00}");
    }
}

/// <summary>Gets messages published with a delay, when the delay is up.</summary>
[QueueName("example_reminders")]
public sealed class ReminderHandler(ActivityFeed feed, TimeProvider time) : IPgmqConsumer<Reminder>
{
    public Task HandleAsync(PgmqConsumeContext<Reminder> context, CancellationToken cancellationToken)
    {
        TimeSpan waited = time.GetUtcNow() - context.Message.ScheduledAt;
        feed.Add(nameof(ReminderHandler), $"Reminder: {context.Message.Text} (after {waited.TotalSeconds:0.0} s)");
        return Task.CompletedTask;
    }
}
