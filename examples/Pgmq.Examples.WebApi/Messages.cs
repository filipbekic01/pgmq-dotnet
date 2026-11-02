namespace Pgmq.Examples.WebApi;

// Messages are plain records. Each consumer of a type gets its own copy of every message published with that type.
// [MessageName] pins the name stored in the database, so the record can be renamed or moved later.

/// <summary>A customer placed an order.</summary>
[MessageName("example.order-placed")]
public sealed record OrderPlaced(Guid OrderId, string Customer, decimal Total, int PaymentFailures);

/// <summary>Charge the customer for an order. Sent by <see cref="OrderPlacedHandler"/>.</summary>
/// <param name="FailTimes">How many calls to the fake payment gateway time out before one succeeds.</param>
[MessageName("example.payment-requested")]
public sealed record PaymentRequested(Guid OrderId, decimal Amount, int FailTimes);

/// <summary>Something to tell someone. Published by type, or to a topic routing key.</summary>
[MessageName("example.notification")]
public sealed record Notification(string Text);

/// <summary>One entry on an account's ledger. Entries of one account must be handled in order.</summary>
[MessageName("example.ledger-entry")]
public sealed record LedgerEntry(string Account, int Sequence, decimal Amount);

/// <summary>A message published now and delivered later.</summary>
[MessageName("example.reminder")]
public sealed record Reminder(string Text, DateTimeOffset ScheduledAt);
