using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// One connection that LISTENs to the insert notifications of every queue that has them, and wakes that queue's read
/// loop. It only shortens an idle wait: a notification lost while it reconnects costs at most one poll interval, and
/// every queue is woken after a reconnect to catch up.
/// </summary>
internal sealed class InsertListener(NpgsqlDataSource dataSource, ILogger logger)
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

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

                foreach (Action wake in wakeByChannel.Values)
                {
                    wake();
                }

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
                logger.LogWarning(ex, "Listening for queue inserts failed; polling until it reconnects in {Delay}",
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
        }
    }
}
