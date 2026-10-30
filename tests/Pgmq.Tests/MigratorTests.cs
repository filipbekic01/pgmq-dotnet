namespace Pgmq.Tests;

public sealed class MigratorTests
{
    private static readonly IReadOnlyList<PgmqMigrator.UpgradeScript> Upgrades =
    [
        PgmqMigrator.UpgradeScript.Parse("pgmq--1.11.0--1.11.1.sql"),
        PgmqMigrator.UpgradeScript.Parse("pgmq--1.11.1--1.12.0.sql"),
        PgmqMigrator.UpgradeScript.Parse("pgmq--1.12.0--1.13.0.sql"),
    ];

    [Fact]
    public void The_embedded_scripts_Should_install_the_version_in_their_control_file()
        => Assert.Equal(new Version(1, 13, 1), PgmqMigrator.EmbeddedVersion);

    [Fact]
    public void The_embedded_upgrade_scripts_Should_form_one_unbroken_chain_up_to_the_embedded_version()
    {
        IReadOnlyList<PgmqMigrator.UpgradeScript> scripts = PgmqMigrator.LoadUpgradeScripts();

        Assert.NotEmpty(scripts);
        Assert.Equal(scripts.Count, scripts.Select(s => s.From).Distinct().Count());
        Assert.Equal(PgmqMigrator.EmbeddedVersion, scripts[^1].To);

        // From 1.0.0 on every step follows the one before; earlier versions cannot be upgraded anyway (upstream).
        PgmqMigrator.UpgradeScript[] modern = [.. scripts.Where(s => s.From >= new Version(1, 0, 0))];
        for (int i = 1; i < modern.Length; i++)
        {
            Assert.Equal(modern[i - 1].To, modern[i].From);
        }

        // Some upstream steps only bump the version and are empty, but every one must be readable.
        Assert.All(scripts, s => Assert.NotNull(PgmqMigrator.ReadResource(s.Name)));
        Assert.Contains("pgmq.meta", PgmqMigrator.ReadResource(PgmqMigrator.INIT_SCRIPT));
    }

    [Fact]
    public void An_empty_database_Should_get_the_install_script_only()
    {
        IReadOnlyList<PgmqMigrator.PlannedScript> plan = PgmqMigrator.Plan(null, new(1, 13, 0), Upgrades, []);

        Assert.Equal([new PgmqMigrator.PlannedScript("pgmq.sql", new(1, 13, 0))], plan);
    }

    [Fact]
    public void An_older_install_Should_get_every_upgrade_script_from_its_version_in_order()
    {
        IReadOnlyList<PgmqMigrator.PlannedScript> plan =
            PgmqMigrator.Plan(new(1, 11, 1), new(1, 13, 0), Upgrades, ["pgmq.sql"]);

        Assert.Equal(["pgmq--1.11.1--1.12.0.sql", "pgmq--1.12.0--1.13.0.sql"], plan.Select(p => p.Name));
        Assert.Equal(new Version(1, 13, 0), plan[^1].To);
    }

    [Fact]
    public void A_current_or_newer_install_Should_get_nothing()
    {
        Assert.Empty(PgmqMigrator.Plan(new(1, 13, 0), new(1, 13, 0), Upgrades, []));
        Assert.Empty(PgmqMigrator.Plan(new(1, 14, 0), new(1, 13, 0), Upgrades, []));
    }

    [Fact]
    public void An_already_applied_script_Should_not_run_again()
    {
        IReadOnlyList<PgmqMigrator.PlannedScript> plan = PgmqMigrator.Plan(new(1, 11, 0), new(1, 12, 0), Upgrades,
            ["pgmq--1.11.0--1.11.1.sql"]);

        Assert.Equal(["pgmq--1.11.1--1.12.0.sql"], plan.Select(p => p.Name));
    }

    [Fact]
    public void A_version_with_no_upgrade_path_Should_fail()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => PgmqMigrator.Plan(new(1, 10, 0), new(1, 13, 0), Upgrades, []));

        Assert.Contains("1.10.0", ex.Message);
    }
}
