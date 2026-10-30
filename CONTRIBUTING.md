# Contributing

Thanks for helping. Issues and pull requests are welcome.

## Prerequisites

- The .NET 10 SDK (`global.json` pins the feature band).
- Docker, for the integration tests and the benchmark.

## Build, format and test

```bash
dotnet restore
dotnet format --verify-no-changes
dotnet build -c Release
dotnet test --project tests/Pgmq.Tests -c Release
dotnet test --project tests/Pgmq.Hosting.Tests -c Release
dotnet test --project tests/Pgmq.IntegrationTests -c Release
```

Release builds treat warnings as errors, and public APIs need XML documentation. `dotnet format` fixes most style
issues.

## Pull requests

- Keep a pull request to one change, with tests: unit tests where the logic allows, integration tests for anything that
  depends on PGMQ.
- If it can change throughput, run the benchmark before and after on the same machine and include both results.
- Add a line to the `Unreleased` section of `CHANGELOG.md`.
- Do not add a dependency or a public abstraction until a feature needs it.

## Releasing

Packages go to NuGet.org only through the **Publish to NuGet** workflow, run by hand from the Actions tab on `main`.

1. Set `<Version>` in `Directory.Build.props`, or pass a version when starting the workflow.
2. Move the `Unreleased` entries in `CHANGELOG.md` under that version.
3. Start the workflow. Tick **dry-run** first to build, test and pack without pushing; the packages are attached to
   the run.

Publishing is keyless, through [NuGet trusted publishing](https://learn.microsoft.com/nuget/nuget-org/trusted-publishing):
GitHub vouches for the workflow and NuGet.org returns an API key that lives for one hour. It relies on a trusted
publishing policy on nuget.org (owner `filipbekic01`, repository `pgmq-dotnet`, workflow file `publish.yml`, environment
`nuget`) and on the repository variable `NUGET_USER`, the nuget.org profile name. The job runs in the `nuget`
environment, which only `main` may deploy to. A version already on NuGet.org is skipped, never overwritten.
