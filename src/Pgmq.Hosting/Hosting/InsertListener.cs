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

    /// <summary>The unpooled source, until it could not connect where the pool can: then null, and the pool is used.</summary>
    private NpgsqlDataSource? _own = database.ListenerDataSource;

    /// <summary>Records that <paramref name="queue"/> has its insert trigger.</summary>
    public void MarkEnabled(string queue) => _enabled[queue] = 0;

    /// <summary>Whether <paramref name="queue"/> has its insert trigger, whether or not the listener is up.</summary>
    public bool IsEnabled(string queue) => _enabled.ContainsKey(queue);

    /// <summary>Whether an insert into <paramref name="queue"/> wakes its read loop right now.</summary>
    public bool IsLive(string queue) => _listening && _enabled.ContainsKey(queue);

    public async Task RunAsync(IReadOnlyDictionary<string, Action> wakeByQueue, Action changed,
        CancellationToken stoppingToken)
    {
        Dictionary<string, Action> wakeByChannel = wakeByQueue.ToDictionary(
            q => PgmqQueueManager.InsertChannel(q.Key), q => q.Value, StringComparer.Ordinal);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using NpgsqlConnection connection = await OpenAsync(stoppingToken);
                connection.Notification += (_, e) =>
                {
                    if (wakeByChannel.TryGetValue(e.Channel, out Action? wake))
                    {
                        wake();
                    }
                };

                foreach (string channel in wakeByChannel.Keys)
                {
                    await using NpgsqlCommand listen = new($"LISTEN {PgmqMigrator.QuoteIdentifier(channel)}", connection);
                    await listen.ExecuteNonQueryAsync(stoppingToken);
                }

                // Live from here. One sweep catches what was inserted before, instead of every queue reading at once.
                _listening = true;
                changed();

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

    /// <summary>
    /// A connection of its own, outside the pool. One built from a handed-in data source's connection string can lack
    /// the password (or a certificate, or anything else set in code); when it fails where the pool connects, the
    /// listener stays on the pool from then on.
    /// </summary>
    private async Task<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        if (_own is not { } own)
        {
            return await database.DataSource.OpenConnectionAsync(cancellationToken);
        }

        try
        {
            return await own.OpenConnectionAsync(cancellationToken);
        }
        catch (Exception ex) when (database.ListenerIsDerived && ex is not OperationCanceledException)
        {
            NpgsqlConnection pooled = await database.DataSource.OpenConnectionAsync(cancellationToken);
            _own = null;
            logger.LogWarning(ex, "Could not open the insert listener's own connection from the data source's " +
                                  "connection string, which keeps the password only with Persist Security Info=true; " +
                                  "listening on a pooled connection instead");
            return pooled;
        }
    }
}
