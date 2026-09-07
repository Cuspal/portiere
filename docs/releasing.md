# Publishing the stabilization release

The working version remains 0.5.0 until a release is prepared. The planned first patch is 0.5.1; confirm that version is still unused on PyPI before selecting it. Source-scoped mapping bundles and the new workflow belong to later minor releases.

## Before selecting a version

1. Complete the stabilization checks in [CONTRIBUTING.md](../CONTRIBUTING.md), including an installed-wheel quickstart outside the checkout. Review the GitHub CI results for the exact commit, including the OS integration matrix. Record which optional backends remain untested.
2. Review the changes and migration notes. Quickstart now fails on incomplete runs, and validation rejects empty output directories; automation that relied on a false success must be updated.
3. Verify the local author and committer with `git var GIT_AUTHOR_IDENT` and `git var GIT_COMMITTER_IDENT`. For Tharathip's releases, both must be Tharathip's own configured identity. Verify GitHub authentication with `gh auth status` and `gh api user --jq .login`; do not switch to another account to work around an authentication failure.
4. Verify repository access and the existing PyPI/TestPyPI project configuration. The current workflow is `.github/workflows/publish.yml`, repository `Cuspal/portiere`, environments `testpypi` and `pypi`. Configure separate trusted publishers on each index with those exact values. Protect the production environment with a required reviewer so publication waits for TestPyPI verification.

The [PyPI trusted publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/) documents GitHub Actions OIDC publishing. Do not put a PyPI token in source control.

## Prepare and verify the candidate

1. Set the chosen version in `pyproject.toml` and `src/portiere/__init__.py`, and move the Unreleased changelog entries under that version and date. Check for any other active version declarations before committing.
2. Run checks again, build the wheel and sdist, inspect their metadata and bundled files, and install the wheel into a fresh environment. Confirm the import version matches distribution metadata and run `portiere quickstart` with the `[polars,quality]` dependencies. A successful import alone is insufficient.
3. Commit using Tharathip's local identity and prepare a release PR with validation evidence. Merge only after the reviewed commit passes CI. Use a new version and tag; do not overwrite an existing published release.

## Publish with a deliberate final gate

The current workflow starts on **GitHub release publication**, builds distributions, uploads them to TestPyPI, then uploads the same artifacts to PyPI. Creating a draft release does not trigger it. Before publishing the GitHub release, confirm the production environment really requires approval; otherwise both uploads can run immediately.

After the TestPyPI job passes, download and install that exact candidate artifact in a fresh environment, supplying ordinary dependencies from PyPI separately. Check metadata, CLI help, the offline quickstart and its manifest. Approve the production job only after these checks pass. Finally, install the published version from PyPI in another fresh environment and repeat the smoke check.

Before using this workflow for the new version, add automated tag/version consistency checks and a candidate installation gate. Remove `skip-existing: true` from the production upload so a reused version cannot be mistaken for a successful new publication. Until those release gates and CI evidence are ready, keep the work under Unreleased.
