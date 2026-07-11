# Gateway publish integration test

End-to-end test that exercises `cvmfs-prepub` publishing through a **real**
`cvmfs_gateway` + `cvmfs_receiver`, backed by S3 (Garage). The Go unit tests
only cover the client side of the lease/payload/commit API; this is the only
test that drives the live path against the actual gateway, including the
DirectGraft fast-path commit.

## What it brings up

The stack combines two fixtures from the cvmfs source tree:

- the S3 backend (Garage) from `test/common/container/s3-integration`
- a **mountless** gateway (`cvmfs_server mkfs -P -D` — no FUSE, no systemd, no
  privileged) from `test/common/container/publish-mountless`, built from
  `cvmfs@devel`

There is **no** `cvmfs_server` publisher container: the publisher is
`cvmfs-prepub`, which speaks the gateway lease/payload/commit API directly.
`prepub` runs on the host and reaches the gateway on the published port `4929`.

```
  cvmfs-prepub (host, gateway mode)
        │  lease → payload → commit/graft   (HMAC-signed HTTP :4929)
        │  ▲ reads .cvmfspublished for old_root_hash (web endpoint :3902)
        ▼  │
  cvmfs_gateway ── spawns ──▶ cvmfs_receiver      (mountless container)
        │                                           writes objects/catalogs
        ▼
  Garage (S3)  ◀── clients read repo data          (web endpoint :3902)
```

`prepub` is started with `--stratum0-url http://localhost:3902` (Garage's web
endpoint). This is **required**: prepub only builds the subtree catalog — the
one that yields `new_root_hash` — when a stratum0 URL is configured. Without it
the commit carries a null `new_root_hash` and the receiver rejects the
DirectGraft with `merge_error` / "DirectGraft requires a catalog hash". For a
fresh repo the manifest GET returns 404 (Garage routes buckets by Host header),
which prepub treats as "first publish" (empty `old_root_hash`); the receiver
fetches the real base manifest itself, so that is correct here.

## Running locally

Requires Docker (with Compose v2), Go, and a cvmfs checkout on `devel`:

```sh
CVMFS_SRC=/path/to/cvmfs ./run.sh
```

The first run builds the gateway image from cvmfs source, which is slow. Set
`KEEP_UP=1` to leave the stack and `prepub` running afterwards for debugging:

```sh
KEEP_UP=1 CVMFS_SRC=/path/to/cvmfs ./run.sh
# ... poke at http://localhost:4929 / http://localhost:8080 ...
CVMFS_SRC=/path/to/cvmfs docker compose -f docker-compose.yml down -v
```

## In CI

`.github/workflows/gateway-publish.yml` checks out both repos, then runs
`run.sh`. It triggers on `workflow_dispatch` (with an optional `cvmfs_ref`
input) and on pull requests that touch the gateway client (`internal/lease`,
`internal/api`, `cmd/prepub`) or these fixtures. It does **not** run on every
push — building cvmfs from source is expensive.

## Credentials

All dev/test only, and must stay in sync across the pieces:

| Secret | Where | Value |
| --- | --- | --- |
| S3 access/secret | `docker-compose.yml` ↔ `garage-setup` ↔ gateway | fixed in compose |
| Gateway lease key | gateway entrypoint writes it to `/etc/cvmfs/keys/<repo>.gw`; prepub signs with the same `CVMFS_GATEWAY_KEY_ID`/`CVMFS_GATEWAY_SECRET` in `run.sh` | `mykey` / `mysecret` |
| prepub API token | `run.sh` `PREPUB_API_TOKEN` | `integration-test-token` |

> The gateway only accepts a lease key that its **access config** associates
> with the repository. The gateway image bakes in `gateway/config/repo.json`,
> which only knows `example_repo.domain.org`, so `docker-compose.yml` mounts the
> mountless `config/repo.json` (which lists `test.repo.org`) and `config/user.json`
> (`enable_key_endpoint`) over it. Without that mount every lease is rejected
> with `invalid key ID specified`, regardless of the key value.
