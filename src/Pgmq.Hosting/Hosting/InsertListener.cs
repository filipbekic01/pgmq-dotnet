using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// One connection that LISTENs to the insert notifications of every queue that has them, and wakes that queue's read
/// loop. While it listens, those queues wait for a notification; while it does not (before it connects, or between a
/// lost connection and the next one), <see cref="IsLive"/> is false and they long-poll instead. Every queue is woken
/// whenever the listener comes up or goes down, so it switches at once, and after a reconnect it catches up on what it
/// missed.
/// </summary>
internal sealed class InsertListener(NpgsqlDataSource dataSource, ILogger<InsertListener> logger)
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    /// <summary>Queues whose insert trigger was set up, so a notification will come for them.</summary>
    private readonly ConcurrentDictionary<string, byte> _enabled = new(StringComparer.Ordinal);

    private volatile bool _listening;

    /// <summary>Records that <paramref name="queue"/> has its insert trigger.</summary>
    public void MarkEnabled(string queue) => _enabled[queue] = 0;

    /// <summary>Whether <paramref name="queue"/> has its insert trigger, whether or not the listener is up.</summary>
    public bool IsEnabled(string queue) => _enabled.ContainsKey(queue);

    /// <summary>Whether an insert into <paramref name="queue"/> wakes its read loop right now.</summary>
    public bool IsLive(string queue) => _listening && _enabled.ContainsKey(queue);

    public async Task RunAsync(IReadOnlyDictionary<string, Action> wakeByQueue, CancellationToken stoppingToken)
    {
        Dictionary<string, Action> wakeByChannel = wakeByQueue.ToDictionary(
            q => PgmqQueueManager.InsertChannel(q.Key), q => q.Value, StringComparer.Ordinal);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(stoppingToken);
                connection.Notification += (_, e) =>
                {
                    if (wakeByChannel.TryGetValue(e.Channel, out Action? wake))
                    {
                        wake();
                    }
                };

                foreach (string channel in wakeByChannel.Keys)
                {
                    await using NpgsqlCommand listen = new($"LISTEN \"{channel.Replace("\"", "\"\"", StringComparison.Ordinal)}\"", connection);
                    await listen.ExecuteNonQueryAsync(stoppingToken);
                }

                // Live from here: a queue reads once more, then waits for its next notification.
                _listening = true;
                WakeAll(wakeByChannel);

                while (!stoppingToken.IsCancellationRequested)
                {
                    await connection.WaitAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Down: wake every queue so it stops waiting for a notification and long-polls instead.
                _listening = false;
                WakeAll(wakeByChannel);

                logger.LogWarning(ex, "Listening for queue inserts failed; long-polling until it reconnects in {Delay}",
                    ReconnectDelay);

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

    private static void WakeAll(Dictionary<string, Action> wakeByChannel)
    {
        foreach (Action wake in wakeByChannel.Values)
        {
            wake();
        }
    }
}
