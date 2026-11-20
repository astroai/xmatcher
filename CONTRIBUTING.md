# Contributing to xmatcher

Thank you for contributing to `xmatcher`!

Before publishing, build the wheel and source archive with `uv build`, then run
`pixi run python scripts/check_release.py`. This verifies current source bytes,
package data, metadata, and the fixtures/helpers needed to run the archived tests.

## Development Setup

We recommend [Pixi](https://pixi.sh) for a reproducible environment with all compiled dependencies (`polars`, `scipy`, `astropy`, `cdshealpix`, `ray`):

```bash
git clone https://github.com/astroai/xmatcher.git
cd xmatcher
pixi install --frozen
```

On shared or NFS-mounted systems (such as CANFAR), isolate user site-packages before running Python or tests:

```bash
export PYTHONNOUSERSITE=1
unset PYTHONPATH
```

## Linting, Formatting, and Tests

Install the checked-in pre-push hook once per clone so pushes fail locally on
format/lint/mypy instead of in CI:

```bash
pixi run install-git-hooks
```

That sets `core.hooksPath` to `scripts/git-hooks/`, where `pre-push` runs
`pixi run --frozen preflight-push` (pixi is required). That hook is the only installed one. The extra
whitespace/YAML/codespell checks in `.pre-commit-config.yaml` are run on demand
with `pre-commit run --all-files`; do not `pre-commit install`, which refuses to
run while `core.hooksPath` is set.

Run the fast preflight after a small change. It checks Ruff lint, formatting, Mypy, and bytecode compilation:

```bash
pixi run preflight-push
```

Auto-format and lint with Ruff:

```bash
pixi run format
pixi run lint
```

Always run `pixi run format` (or `preflight-push`) before pushing pensus or other
script changes; CI's format check rejects unformatted Python.

Run the release gate before opening a pull request. It runs the preflight and all tests not marked `slow` or `bench`:

```bash
pixi run ci-local
```

Run targeted tests while developing, or select the full suite explicitly when you need slow and benchmark tests:

```bash
# Targeted module tests
pixi run test tests/test_matchers.py -q

# Entire pytest suite, including tests excluded from ci-local
pixi run test
```

torchsky is a core dependency, so the default environment runs every torchsky path (matching, association statistics, epoch Jacobians, HEALPix). Until torchsky 0.6.0 is on PyPI, Pixi builds it from the release-candidate commit pinned in `pixi.toml`, against the environment's conda-forge CPU PyTorch.

The offline `ci-local` gate does not establish that optional integrations, remote services, credentials, or every Python version work. Run the relevant integration checks separately when changing those paths. The checked Pixi environment uses Python 3.13.

## Pull Requests

- Follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `perf:`, `refactor:`, `test:`).
- Ensure `pixi run ci-local` and any relevant optional integration checks pass before opening a pull request against `astroai/xmatcher`.
