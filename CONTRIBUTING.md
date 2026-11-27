# Contributing to xmatcher

Thank you for contributing to `xmatcher`! User-facing installation is in
[docs/install.md](docs/install.md); this page covers development.

## Development setup

Use [Pixi](https://pixi.sh) for the locked environment. It includes Python
3.13, conda-forge CPU PyTorch, torchsky, torchfits, HATS, Ray and astroquery.
The optional `ml` extra adds the machine-learning matchers.

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

## Linting, formatting, and tests

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

Always run `pixi run format` (or `preflight-push`) before pushing changes under
`scripts/`; CI's format check rejects unformatted Python.

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

torchsky is a core dependency, so the default environment runs every torchsky path (matching, association statistics, epoch Jacobians, HEALPix). Pixi installs the torchsky wheel from PyPI next to the environment's conda-forge CPU PyTorch.

The offline `ci-local` gate does not establish that optional integrations, remote services, credentials, or every Python version work. Run the relevant integration checks separately when changing those paths. The checked Pixi environment uses Python 3.13.

## Releases

Release notes go in `docs/archive/release-X.Y.Z.md`, and the `CHANGELOG.md`
section gets its date in the release pull request. Before tagging, build the
wheel and source archive and verify them:

```bash
uv build --out-dir dist
pixi run python scripts/check_release.py --dist dist --tag vX.Y.Z
```

This checks the source bytes, package data, metadata, and the fixtures and
helpers the shipped tests need. Pushing a `vX.Y.Z` tag on `main` runs the same
gate in CI, publishes the GitHub release with those notes, and uploads the
tested archives to PyPI.

## Pull requests

- Follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `perf:`, `refactor:`, `test:`).
- Ensure `pixi run ci-local` and any relevant optional integration checks pass before opening a pull request against `astroai/xmatcher`.
