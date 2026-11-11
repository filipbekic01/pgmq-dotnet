using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// One connection that LISTENs to the insert notifications of every queue that has them, and wakes that queue's read
/// loop. It is opened outside the pool (<see cref="PgmqDatabase.ListenerDataSource"/>), so it never takes a pool slot
/// and never waits for one when it reconnects. While it listens, <see cref="IsLive"/> is true for those queues; while
/// it does not (before it connects, or between a lost connection and the next one), it is false and the sweeper looks
/// for work more often. Each time it comes up or goes down it calls <c>changed</c>, so the sweeper sweeps at once and
/// catches what was inserted while nobody listened.
/// </summary>
internal sealed class InsertListener(PgmqDatabase database, ILogger<InsertListener> logger)
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    /// <summary>How long the connection may stay quiet before it is pinged to prove it is still there.</summary>
    private static readonly TimeSpan PingAfter = TimeSpan.FromSeconds(30);

    /// <summary>How long a ping may take before the connection counts as dead.</summary>
    private const int PingTimeoutSeconds = 10;

    /// <summary>Queues whose insert trigger was set up, so a notification will come for them.</summary>
    private readonly ConcurrentDictionary<string, byte> _enabled = new(StringComparer.Ordinal);

    private volatile bool _listening;

    /// <summary>How long after the throttle interval ends the sweep for what it swallowed runs.</summary>
    private static readonly TimeSpan ThrottleMargin = TimeSpan.FromMilliseconds(50);

    /// <summary>1 while a sweep for what the notification throttle swallowed is waiting to run.</summary>
    private int _sweepPending;

    /// <summary>Records that <paramref name="queue"/> has its insert trigger.</summary>
    public void MarkEnabled(string queue) => _enabled[queue] = 0;

    /// <summary>Whether <paramref name="queue"/> has its insert trigger, whether or not the listener is up.</summary>
    public bool IsEnabled(string queue) => _enabled.ContainsKey(queue);

    /// <summary>Whether an insert into <paramref name="queue"/> wakes its read loop right now.</summary>
    public bool IsLive(string queue) => _listening && _enabled.ContainsKey(queue);

    /// <summary>
    /// Asks for one sweep when the throttle time is over, for the inserts pgmq sent no notification for. One sweep
    /// covers every queue, however many were notified meanwhile.
    /// </summary>
    private void SweepAfterThrottle(TimeSpan throttle, Action sweep, CancellationToken stoppingToken)
    {
        if (throttle <= TimeSpan.Zero || Interlocked.CompareExchange(ref _sweepPending, 1, 0) != 0)
        {
            return;
        }

        _ = Task.Delay(throttle + ThrottleMargin, stoppingToken).ContinueWith(_ =>
        {
            Volatile.Write(ref _sweepPending, 0);
            sweep();
        }, TaskScheduler.Default);
    }

    /// <param name="wakeByQueue">How to wake each queue's read loop.</param>
    /// <param name="changed">Makes the sweeper sweep at once.</param>
    /// <param name="throttle">
    /// The shortest time between two notifications of one queue (<see cref="PgmqOptions.InsertNotificationThrottle"/>).
    /// pgmq sends none for an insert inside that time and none when it ends, so a message inserted just after another
    /// would wait for the next regular sweep. One sweep is asked for when the time is over instead.
    /// </param>
    /// <param name="stoppingToken">Ends the listener.</param>
    public async Task RunAsync(IReadOnlyDictionary<string, Action> wakeByQueue, Action changed, TimeSpan throttle,
        CancellationToken stoppingToken)
    {
        Dictionary<string, Action> wakeByChannel = wakeByQueue.ToDictionary(
            q => PgmqQueueManager.InsertChannel(q.Key), q => q.Value, StringComparer.Ordinal);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using NpgsqlConnection connection = await database.OpenDedicatedAsync(logger, stoppingToken);
                connection.Notification += (_, e) =>
                {
                    if (!wakeByChannel.TryGetValue(e.Channel, out Action? wake))
                    {
                        return;
                    }

                    wake();
                    SweepAfterThrottle(throttle, changed, stoppingToken);
                };

                foreach (string channel in wakeByChannel.Keys)
                {
                    await using NpgsqlCommand listen = new($"LISTEN {PgmqMigrator.QuoteIdentifier(channel)}", connection);
                    await listen.ExecuteNonQueryAsync(stoppingToken);
                }

                // Live from here. One sweep catches what was inserted before, instead of every queue reading at once.
                _listening = true;
                changed();

                // A notification sent just before this connection listened was heard by nobody, and still throttles
                // the inserts after it.
                SweepAfterThrottle(throttle, changed, stoppingToken);

                // A connection that dies without being closed (an idle timeout in a NAT or load balancer, a failover)
                // would wait here forever, still counted as live. A quiet connection is pinged, so a dead one fails
                // and is opened again.
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (!await connection.WaitAsync(PingAfter, stoppingToken))
                    {
                        await using NpgsqlCommand ping = new("select 1", connection);
                        ping.CommandTimeout = PingTimeoutSeconds;
                        await ping.ExecuteNonQueryAsync(stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Down: the sweeper switches to its fast interval until the listener is back.
                _listening = false;
                changed();

                logger.LogWarning(ex, "Listening for queue inserts failed; reconnecting in {Delay}", ReconnectDelay);

                try
                {
                    await Task.Delay(ReconnectDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            finally
            {
                _listening = false;
            }
        }
    }
}
