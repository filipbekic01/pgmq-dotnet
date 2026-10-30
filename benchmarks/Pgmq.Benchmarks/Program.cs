using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Pgmq;
using Pgmq.Benchmarks;

// Throughput benchmark: pushes messages through PGMQ queues whose consumers do nothing, and compares the speed with
// the committed baseline (baseline.json beside this file). Needs Docker; starts its own PGMQ container.
//
//   dotnet run -c Release --project benchmarks/Pgmq.Benchmarks
//   dotnet run -c Release --project benchmarks/Pgmq.Benchmarks -- --messages 100000 --queues 100 --runs 3 --instances 1
//   dotnet run -c Release --project benchmarks/Pgmq.Benchmarks -- --save-baseline
//   dotnet run -c Release --project benchmarks/Pgmq.Benchmarks -- --matrix
//   dotnet run -c Release --project benchmarks/Pgmq.Benchmarks -- --matrix --queues 1,10,100 --messages 50000
//
// --matrix pushes the same messages through each queue count in turn (default 1, 10, 50, 100, 200; one run each
// unless --runs is given) and writes the chart in the README to docs/benchmark.svg, or to --chart.
// --notifications off measures without LISTEN/NOTIFY, so long polling alone.
//
// Numbers are only comparable on the same machine: save the baseline where you compare, and compare before and
// after a change on that machine. Exit code 1 if a run lost, duplicated or archived a message, or logged an error;
// 2 if --fail-on-regression is given and a number got worse than --threshold percent.

Dictionary<string, string?> options = ParseArgs(args);
bool matrix = options.ContainsKey("matrix");
int messages = Int("messages", 100_000);
int[] queueCounts = matrix
    ? [.. (options.GetValueOrDefault("queues") ?? "1,10,50,100,200").Split(',').Select(q => int.Parse(q, CultureInfo.InvariantCulture))]
    : [Int("queues", 100)];
int queues = queueCounts.Max();
Action<PgmqOptions>? configure = options.GetValueOrDefault("notifications") == "off"
    ? o => o.UseInsertNotifications = false
    : null;
int runs = Int("runs", 3);
int instances = Int("instances", 1);
double threshold = double.Parse(options.GetValueOrDefault("threshold") ?? "10", CultureInfo.InvariantCulture);
LoadMode[] modes = options.GetValueOrDefault("mode") switch
{
    "drain" => [LoadMode.Drain],
    "live" => [LoadMode.Live],
    _ => [LoadMode.Drain, LoadMode.Live],
};

if (matrix)
{
    runs = options.ContainsKey("runs") ? runs : 1;
}

Console.WriteLine($"Pgmq benchmark: {messages:N0} messages over {string.Join(", ", queueCounts)} queues, {instances} instance(s), {runs} runs per mode, " +
                  $"{Environment.ProcessorCount} cores, {RuntimeInformation.OSDescription}");

await using PgmqContainer container = new();
string connectionString = options.GetValueOrDefault("connection") ?? await StartContainerAsync(container);

if (matrix)
{
    (List<MatrixRow> rows, bool matrixClean) = await Matrix.RunAsync(connectionString, messages, queueCounts, runs, instances, configure);
    string chartPath = options.GetValueOrDefault("chart") ?? Path.Combine(RepositoryRoot(), "docs", "benchmark.svg");
    File.WriteAllText(chartPath, Matrix.Svg(rows, messages));
    Console.WriteLine($"Wrote the chart to {chartPath}");

    if (!matrixClean)
    {
        Console.WriteLine("FAILED: a run lost, duplicated or archived messages, left some on a queue, or logged an error.");
        return 1;
    }

    return 0;
}

Dictionary<LoadMode, LoadResult> medians = [];
bool clean = true;

foreach (LoadMode mode in modes)
{
    // A small warmup run: JIT, connection pool and Postgres caches, none of it measured.
    await new LoadScenario(connectionString, Math.Min(messages, 5_000), queues, configure) { Instances = instances }.RunAsync(mode);

    List<LoadResult> results = [];
    for (int run = 1; run <= runs; run++)
    {
        LoadResult result = await new LoadScenario(connectionString, messages, queues, configure) { Instances = instances }.RunAsync(mode);
        results.Add(result);
        Console.WriteLine($"  {mode} run {run}: {Describe(result)}");
        clean &= result.Clean;
    }

    medians[mode] = results.OrderBy(r => r.ConsumePerSecond).ElementAt(results.Count / 2);
}

JsonObject now = ToJson(medians, messages, queues);
string baselinePath = BaselinePath();
JsonObject? baseline = File.Exists(baselinePath) ? JsonNode.Parse(File.ReadAllText(baselinePath))?.AsObject() : null;

Console.WriteLine();
bool regressed = PrintComparison(now, baseline, threshold);

if (options.ContainsKey("save-baseline"))
{
    File.WriteAllText(baselinePath, now.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) + "\n");
    Console.WriteLine($"Saved baseline to {baselinePath}");
}

if (!clean)
{
    Console.WriteLine("FAILED: a run lost, duplicated or archived messages, left some on a queue, or logged an error.");
    return 1;
}

return regressed && options.ContainsKey("fail-on-regression") ? 2 : 0;

int Int(string name, int fallback)
    => options.TryGetValue(name, out string? value) && value is not null ? int.Parse(value, CultureInfo.InvariantCulture) : fallback;

static async Task<string> StartContainerAsync(PgmqContainer container)
{
    Console.WriteLine($"Starting {PgmqContainer.Image}...");
    await container.StartAsync();
    return container.ConnectionString;
}

static string Describe(LoadResult r)
    => $"publish {r.PublishPerSecond,8:N0}/s  consume {r.ConsumePerSecond,8:N0}/s  " +
       (r.Mode == LoadMode.Live
           ? $"latency p50 {r.LatencyP50.TotalMilliseconds:N0}ms p95 {r.LatencyP95.TotalMilliseconds:N0}ms p99 {r.LatencyP99.TotalMilliseconds:N0}ms  "
           : "") +
       (r.Clean
           ? "clean"
           : $"lost {r.Lost} dup {r.Duplicates} left {r.LeftOnQueues} archived {r.Archived} errors {r.ErrorsLogged}") +
       (r.WarningsLogged > 0 ? $" ({r.WarningsLogged} warnings)" : "");

static JsonObject ToJson(Dictionary<LoadMode, LoadResult> medians, int messages, int queues)
{
    JsonObject results = [];
    foreach ((LoadMode mode, LoadResult r) in medians)
    {
        JsonObject entry = new()
        {
            ["publishPerSecond"] = Math.Round(r.PublishPerSecond),
            ["consumePerSecond"] = Math.Round(r.ConsumePerSecond),
        };

        if (mode == LoadMode.Live)
        {
            entry["latencyP50Ms"] = Math.Round(r.LatencyP50.TotalMilliseconds);
            entry["latencyP95Ms"] = Math.Round(r.LatencyP95.TotalMilliseconds);
            entry["latencyP99Ms"] = Math.Round(r.LatencyP99.TotalMilliseconds);
        }

        results[mode.ToString().ToLowerInvariant()] = entry;
    }

    return new JsonObject
    {
        ["messages"] = messages,
        ["queues"] = queues,
        ["machine"] = $"{Environment.ProcessorCount} cores, {RuntimeInformation.OSDescription}, {RuntimeInformation.OSArchitecture}",
        ["date"] = DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        ["results"] = results,
    };
}

/// <summary>Prints each number beside the baseline. Throughput should go up, latency down.</summary>
static bool PrintComparison(JsonObject now, JsonObject? baseline, double threshold)
{
    if (baseline is null)
    {
        Console.WriteLine("No baseline yet; run with --save-baseline to keep these numbers.");
    }
    else
    {
        Console.WriteLine($"Baseline: {baseline["date"]}, {baseline["machine"]}, {baseline["messages"]} messages over {baseline["queues"]} queues");
    }

    Console.WriteLine($"{"metric",-28} {"now",10} {"baseline",10} {"change",9}");
    bool regressed = false;

    foreach ((string mode, JsonNode? metrics) in now["results"]!.AsObject())
    {
        foreach ((string metric, JsonNode? value) in metrics!.AsObject())
        {
            double current = value!.GetValue<double>();
            double? before = baseline?["results"]?[mode]?[metric]?.GetValue<double>();
            bool higherIsBetter = metric.EndsWith("PerSecond", StringComparison.Ordinal);

            string change = "";
            if (before is > 0)
            {
                double percent = (current - before.Value) / before.Value * 100;
                bool worse = higherIsBetter ? percent < -threshold : percent > threshold;
                bool better = higherIsBetter ? percent > threshold : percent < -threshold;
                regressed |= worse;
                change = $"{percent,+7:+0.0;-0.0}%{(worse ? " WORSE" : better ? " better" : "")}";
            }

            Console.WriteLine($"{mode + "." + metric,-28} {current,10:N0} {before?.ToString("N0", CultureInfo.InvariantCulture) ?? "-",10} {change}");
        }
    }

    return regressed;
}

static string BaselinePath() => Path.Combine(RepositoryRoot(), "benchmarks", "Pgmq.Benchmarks", "baseline.json");

static string RepositoryRoot()
{
    for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
    {
        if (Directory.Exists(Path.Combine(dir.FullName, "benchmarks", "Pgmq.Benchmarks")))
        {
            return dir.FullName;
        }
    }

    throw new InvalidOperationException("Could not find benchmarks/Pgmq.Benchmarks above the binaries.");
}

static Dictionary<string, string?> ParseArgs(string[] args)
{
    Dictionary<string, string?> parsed = [];
    for (int i = 0; i < args.Length; i++)
    {
        if (!args[i].StartsWith("--", StringComparison.Ordinal))
        {
            continue;
        }

        string name = args[i][2..];
        bool hasValue = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal);
        parsed[name] = hasValue ? args[++i] : null;
    }

    return parsed;
}
