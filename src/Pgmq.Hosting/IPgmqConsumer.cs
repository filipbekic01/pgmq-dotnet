namespace Pgmq;

/// <summary>
/// Handles one message type. Register it with <c>AddPgmqConsumer&lt;TConsumer&gt;()</c>; it then gets its own queue,
/// and publishing a <typeparamref name="TMessage"/> writes a copy to it. Delivery is at least once, so a handler should
/// tolerate seeing the same message twice.
/// </summary>
/// <typeparam name="TMessage">The message type this consumer handles, and the types deriving from it.</typeparam>
public interface IPgmqConsumer<TMessage> where TMessage : class
{
    /// <summary>
    /// Handles one message. Returning deletes it from the queue; throwing retries it when
    /// <see cref="PgmqOptions.IsTransient"/> says the failure is transient, and archives it otherwise.
    /// </summary>
    /// <param name="context">The message and what it arrived with, and ways to publish more.</param>
    /// <param name="cancellationToken">Cancelled when the message runs past its timeout, or the host shuts down.</param>
    Task HandleAsync(PgmqConsumeContext<TMessage> context, CancellationToken cancellationToken);
}
