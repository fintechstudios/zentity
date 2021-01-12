# Releasing zentity

GitHub Actions creates and uploads release artifacts on tags, via the 
[`.github/workflows/release.yaml` workflow](.github/workflows/release.yaml).

## Release branches

When preparing a release, a release-specific branch should be created from the `master` branch.

The release branches are named in the format `release/A.B`, where `A` is the major release number
and `B` is the minor release number. These branches must be stable, as they hold the tagged releases of Zentity.

## Tags

Tags must strictly follow [SemVer v2.0.0](https://semver.org/spec/v2.0.0.html).
The only pre-release identifier is `rcX` where X is a number.

Pre-release tags are in the format `A.B.C-rcX`, ex: `1.0.0-rc1`.
Stable release tags are in the format `A.B.C`, ex: `1.0.0`.

## Releases

The process to release a Zentity version is as follows:
 * Create the release branch `release/A.B` and tag `release/A.B.0-rc1`.
 * Push the tag, which will trigger the release workflow.
 * If a critical problem is found, fix it and release `rc2`.
 * When there is confidence that a release is ready, tag and push `A.B.0`.

All together now:
```bash
# Get onto the lastest master
git fetch --all
git checkout origin/master

# Create and push the release branch
git checkout -b release/A.B
git push -u origin release/A.B

# Tag and push the release
git tag -a A.B.0-rc1 -m "First release candidate for A.B line" 
git push origin A.B.0-rc1
```

### Patch releases

After the initial release there may be a need for a patch release `A.B.1`, `A.B.2` and so on.
These typically include only fixes to critical issues, but there might be exceptions where features are
backported from master. For instance, if a `master` has dropped support for Elasticsearch 7, but a feature is deemed
useful to enough users of es7, the feature can be backported to a release branch that still supports es7 and released
as a patch.
