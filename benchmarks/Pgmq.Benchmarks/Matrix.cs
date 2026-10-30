using System.Globalization;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;

namespace Pgmq.Benchmarks;

/// <summary>One cell of the matrix: a queue count with its drain and live results.</summary>
public sealed record MatrixRow(int Queues, LoadResult Drain, LoadResult Live);

/// <summary>
/// Pushes the same number of messages through 1, 10, 100... queues, in both modes, and draws the result as an SVG
/// bar chart for the README. Shows how throughput spreads across queues and what a message waits in live traffic.
/// </summary>
public static class Matrix
{
    public static async Task<(List<MatrixRow> Rows, bool Clean)> RunAsync(string connectionString, int messages,
        int[] queueCounts, int runs, int instances, Action<PgmqOptions>? configure = null)
    {
        List<MatrixRow> rows = [];
        bool clean = true;

        // A small warmup run: JIT, connection pool and Postgres caches, none of it measured.
        await new LoadScenario(connectionString, Math.Min(messages, 5_000), queueCounts.Max(), configure) { Instances = instances }
            .RunAsync(LoadMode.Live);

        foreach (int queues in queueCounts)
        {
            LoadResult drain = await MedianAsync(LoadMode.Drain);
            LoadResult live = await MedianAsync(LoadMode.Live);
            rows.Add(new(queues, drain, live));
            clean &= drain.Clean && live.Clean;

            Console.WriteLine(
                $"  {queues,4} queues: drain {drain.ConsumePerSecond,8:N0}/s  live {live.ConsumePerSecond,8:N0}/s  " +
                $"p50 {live.LatencyP50.TotalMilliseconds,5:N0}ms  p95 {live.LatencyP95.TotalMilliseconds,5:N0}ms" +
                (drain.Clean && live.Clean ? "" : "  NOT CLEAN"));

            async Task<LoadResult> MedianAsync(LoadMode mode)
            {
                List<LoadResult> results = [];
                for (int run = 0; run < runs; run++)
                {
                    results.Add(await new LoadScenario(connectionString, messages, queues, configure) { Instances = instances }
                        .RunAsync(mode));
                }

                return results.OrderBy(r => r.ConsumePerSecond).ElementAt(results.Count / 2);
            }
        }

        return (rows, clean);
    }

    /// <summary>
    /// A grouped bar chart: messages handled per second for each queue count, drain beside live, with live latency
    /// under each group. Neutral colors on a transparent background, so it reads in GitHub's light and dark themes.
    /// </summary>
    public static string Svg(IReadOnlyList<MatrixRow> rows, int messages)
    {
        const int width = 760, height = 400;
        const int left = 70, right = 20, top = 78, bottom = 92;
        const string drainColor = "#3b82f6", liveColor = "#f59e0b", text = "#8b949e";
        int plotWidth = width - left - right, plotHeight = height - top - bottom;

        double max = NiceCeiling(rows.Max(r => Math.Max(r.Drain.ConsumePerSecond, r.Live.ConsumePerSecond)));
        double groupWidth = (double)plotWidth / rows.Count;
        double barWidth = Math.Min(48, groupWidth * 0.32);

        StringBuilder svg = new();
        svg.Append(Inv($"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif">"""));
        svg.Append(Inv($"""<text x="{left}" y="28" font-size="18" font-weight="600" fill="{text}">Pgmq.NET throughput: {messages:N0} messages over N queues</text>"""));
        svg.Append(Inv($"""<text x="{left}" y="48" font-size="12" fill="{text}">{Escape(Machine())}, default settings, consumers that do nothing</text>"""));

        // Legend.
        svg.Append(Inv($"""<rect x="{width - right - 250}" y="58" width="12" height="12" rx="2" fill="{drainColor}"/>"""));
        svg.Append(Inv($"""<text x="{width - right - 233}" y="68" font-size="12" fill="{text}">Drain (queues full at start)</text>"""));
        svg.Append(Inv($"""<rect x="{width - right - 80}" y="58" width="12" height="12" rx="2" fill="{liveColor}"/>"""));
        svg.Append(Inv($"""<text x="{width - right - 63}" y="68" font-size="12" fill="{text}">Live</text>"""));

        // Grid and y axis.
        for (int i = 0; i <= 4; i++)
        {
            double value = max * i / 4;
            double y = top + plotHeight - plotHeight * i / 4.0;
            svg.Append(Inv($"""<line x1="{left}" x2="{width - right}" y1="{y:0.#}" y2="{y:0.#}" stroke="{text}" stroke-opacity="{(i == 0 ? 0.6 : 0.2)}"/>"""));
            svg.Append(Inv($"""<text x="{left - 8}" y="{y + 4:0.#}" font-size="11" text-anchor="end" fill="{text}">{Short(value)}</text>"""));
        }

        svg.Append(Inv($"""<text transform="translate(16 {top + plotHeight / 2}) rotate(-90)" font-size="12" text-anchor="middle" fill="{text}">messages handled per second</text>"""));

        for (int g = 0; g < rows.Count; g++)
        {
            MatrixRow row = rows[g];
            double center = left + groupWidth * (g + 0.5);
            Bar(center - barWidth - 2, row.Drain.ConsumePerSecond, drainColor);
            Bar(center + 2, row.Live.ConsumePerSecond, liveColor);

            double labelY = top + plotHeight;
            svg.Append(Inv($"""<text x="{center:0.#}" y="{labelY + 20}" font-size="13" font-weight="600" text-anchor="middle" fill="{text}">{row.Queues} {(row.Queues == 1 ? "queue" : "queues")}</text>"""));
            svg.Append(Inv($"""<text x="{center:0.#}" y="{labelY + 38}" font-size="11" text-anchor="middle" fill="{text}">live p50 {row.Live.LatencyP50.TotalMilliseconds:N0} ms</text>"""));
            svg.Append(Inv($"""<text x="{center:0.#}" y="{labelY + 53}" font-size="11" text-anchor="middle" fill="{text}">p95 {row.Live.LatencyP95.TotalMilliseconds:N0} ms</text>"""));
        }

        svg.Append(Inv($"""<text x="{left}" y="{height - 10}" font-size="11" fill="{text}">dotnet run -c Release --project benchmarks/Pgmq.Benchmarks -- --matrix</text>"""));
        svg.Append("</svg>\n");
        return svg.ToString();

        void Bar(double x, double value, string color)
        {
            double h = plotHeight * value / max;
            double y = top + plotHeight - h;
            svg.Append(Inv($"""<rect x="{x:0.#}" y="{y:0.#}" width="{barWidth:0.#}" height="{h:0.#}" rx="3" fill="{color}"/>"""));
            svg.Append(Inv($"""<text x="{x + barWidth / 2:0.#}" y="{y - 5:0.#}" font-size="11" text-anchor="middle" fill="{text}">{Short(value)}</text>"""));
        }
    }

    private static string Machine()
        => $"{Environment.ProcessorCount} cores, {RuntimeInformation.OSDescription}, {RuntimeInformation.OSArchitecture}, " +
           $"{PgmqContainer.Image.Split('/')[^1]}";

    /// <summary>1, 2 or 5 times a power of ten, at least <paramref name="value"/>.</summary>
    private static double NiceCeiling(double value)
    {
        double power = Math.Pow(10, Math.Floor(Math.Log10(Math.Max(value, 1))));
        foreach (double step in (double[])[1, 2, 2.5, 5, 10])
        {
            if (step * power >= value)
            {
                return step * power;
            }
        }

        return 10 * power;
    }

    private static string Short(double value)
        => value >= 1000 ? (value / 1000).ToString(value >= 10_000 ? "0" : "0.#", CultureInfo.InvariantCulture) + "k"
            : value.ToString("0", CultureInfo.InvariantCulture);

    private static string Escape(string value) => SecurityElement.Escape(value);

    private static string Inv(FormattableString value) => value.ToString(CultureInfo.InvariantCulture);
}
