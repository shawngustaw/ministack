# Jump's MiniStack fork

This is upstream MiniStack ([ministackorg/ministack](https://github.com/ministackorg/ministack))
plus a few patches that Jump's `pm local` needs. `pm local` runs Jump's services against
MiniStack on a developer's machine. The branch `jump` is an upstream release with those
patches on top. Each tag builds an image at
`registry.gitlab.com/theworkingname/jump/infra/ministack:<tag>`.

The patches are fixes we need before upstream ships them. Each is one commit with its
tests and a CHANGELOG line, so it can be dropped once upstream has an equivalent.
Upstream's contribution rules want behaviour backed by a capture from real AWS, so these
go upstream as issues, not PRs.

## Patches

Based on upstream release **1.5.41** (`837b11b5`).

| Commit | What it fixes | Upstream issue |
| --- | --- | --- |
| `fix(elasticache): remove a stopped leftover before respawning after a restart` | After a Docker restart, a replication group's Valkey container is not recreated (409 name conflict) and its endpoint stays stale. | ElastiCache respawn name conflict (to be filed) |
| `fix(lambda): equals-ignore-case and absent parents in ESM FilterCriteria` | Event-source-mapping filters with `equals-ignore-case`, or `{"exists": false}` under a missing attribute, never match. | Lambda ESM `equals-ignore-case` (to be filed) |
| `feat(pipes): deliver to SQS queues and EventBridge event buses` | Pipes to an SQS queue or an event bus hold every record. | Pipes event bus and SQS targets (to be filed) |
| `feat(pipes): apply SourceParameters.FilterCriteria` | Pipes deliver every record, ignoring the filter. | Pipes FilterCriteria and InputTemplate (to be filed) |
| `feat(pipes): apply TargetParameters.InputTemplate` | Pipes deliver the raw record, ignoring the input template. | Pipes FilterCriteria and InputTemplate (to be filed) |
| `feat(eventbridge): deliver to Kinesis stream targets` | A rule targeting a Kinesis stream logs "unsupported target type" and delivers nothing (pro-event-bridge's offers go to the pro-offer stream this way). | EventBridge Kinesis targets (to be filed) |
| `fix(rds): Aurora PostgreSQL max_connections as Aurora sizes it` | Aurora PostgreSQL containers get the image's 100 connections; platform-availability's warm Lambda pools exhaust them. | Aurora max_connections (to be filed) |
| `fix(lambda): honour MaximumRetryAttempts on stream mappings` | A failing Kinesis or DynamoDB stream batch is retried forever and blocks its shard (availability's offer consumer stopped on one bad offer). | Stream ESM MaximumRetryAttempts (to be filed) |
| `ci: build Jump's image on GitLab` | This file and `.gitlab-ci.yml`. Jump only. | none |

Fill in the issue links once they are filed. When upstream fixes one, drop its commit at the
next rebase and remove its row.

The Pipes event-bus envelope used when a pipe sets no `EventBridgeEventBusParameters`
(`source` `Pipe <name>`, `detail-type` `Event from <eventSource>`) was not captured from AWS.
Jump's rules match on `detail` only, so it does not matter to us. Check it before it goes
upstream.

## Rebase onto a new upstream release

```sh
git remote add upstream https://github.com/ministackorg/ministack.git   # once
git fetch upstream --tags
git switch jump
git rebase --onto v<new> v1.5.41 jump   # or the upstream release commit
```

1. Drop the commits for anything upstream now fixes (`git rebase -i` locally, or `git rebase
   --onto` past them). CHANGELOG conflicts come from every patch adding a line under
   `[Unreleased]`: keep the patches' lines.
2. In `.gitlab-ci.yml`, point `PLUGIN_DONOR_IMAGE` at the new release's full image, by digest:
   `docker buildx imagetools inspect ghcr.io/ministackorg/ministack:<new>-full`.
3. Update the base version and commit hash above.
4. Run the tests for the patched services against a local server (port 4566 is often taken
   by LocalStack, so use another):

   ```sh
   GATEWAY_PORT=4580 python -m hypercorn ministack.app:app --bind 0.0.0.0:4580 &
   MINISTACK_ENDPOINT=http://localhost:4580 GATEWAY_PORT=4580 \
     pytest tests/test_elasticache.py tests/test_lambda.py tests/test_pipes.py tests/test_eventbridge.py \
     -m "not data_plane and not serial" -n 4
   ruff check ministack/
   ```

5. Build and try the image with `pm local` (below), then push `jump` with `--force-with-lease`
   and cut a tag.

## Cut a tag

Tags are `<upstream version>-jump.<n>`: `n` starts at 1 for each upstream version and goes up
with every Jump build of it.

```sh
git tag -a 1.5.41-jump.2 -m "MiniStack 1.5.41 + Jump patches"
git push origin 1.5.41-jump.2
```

The tag's pipeline builds both architectures and pushes `:<tag>` and `:jump-latest`. Then
update the default image in the lab repo's `LocalSettings` (`pm local`).

## Try a build locally

```sh
docker build -t ministack-jump:local \
  --build-arg PLUGIN_DONOR_IMAGE=ghcr.io/ministackorg/ministack@sha256:<digest from .gitlab-ci.yml> .
PM_LOCAL_MINISTACK_IMAGE=ministack-jump:local pm local up <service>
```

`pm local` recreates its MiniStack container when the image changes. State is kept in its
volume, so `pm local down --yes` first if you want a clean start.
