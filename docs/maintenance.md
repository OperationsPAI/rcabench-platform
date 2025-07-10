# Maintenance Documentation

This document provides guidelines for maintaining the rcabench-platform project, including release procedures and common maintenance tasks.

## Release Process

### Prerequisites

Before releasing a new version, ensure:

1. All tests are passing
2. The main branch is in a stable state
3. You have push access to the repository
4. The working directory is clean (no uncommitted changes)

### Patch Release

For patch releases (bug fixes, minor improvements), use the automated release script:

```bash
./scripts/release-patch.sh
```

#### What the script does:

1. **Switches to main branch**: Ensures you're working from the correct branch
2. **Checks working tree**: Verifies there are no uncommitted changes
3. **Bumps patch version**: Uses `uv version --bump patch` to increment the patch version (e.g., 0.2.10 → 0.2.11)
4. **Commits changes**: Creates a commit with the message "release vX.Y.Z"
5. **Pushes to main**: Uploads the release commit
6. **Creates and pushes tag**: Tags the release and pushes the tag to enable automated deployments

#### Manual patch release steps:

If you need to perform a patch release manually:

```bash
# Ensure you're on main and working tree is clean
git checkout main
git status  # Should show "nothing to commit, working tree clean"

# Bump the patch version
uv version --bump patch
VERSION="v$(uv version --short)"

# Commit and push
git add -A
git commit -m "release $VERSION"
git push origin main

# Tag and push tag
git tag "$VERSION"
git push origin "$VERSION"
```

### Minor/Major Releases

For minor or major version bumps, modify the release script or perform manual steps:

```bash
# For minor version bump (new features)
uv version --bump minor

# For major version bump (breaking changes)
uv version --bump major
```

## Version Management

The project uses semantic versioning (SemVer) with the format `MAJOR.MINOR.PATCH`:

- **PATCH**: Bug fixes and small improvements
- **MINOR**: New features that are backward compatible
- **MAJOR**: Breaking changes

Version information is stored in:
- `pyproject.toml` - The main version field
- Git tags - For release tracking

## Common Maintenance Tasks

### Updating Dependencies

Dependencies are managed using `uv`. To update:

```bash
# Update all dependencies to latest compatible versions
uv lock --upgrade

# Update specific dependency
uv add package_name@latest
```

### Checking Project Health

```bash
# Run tests
python -m pytest

# Check formatting and linting
ruff check .
ruff format --check .

# Type checking (if applicable)
mypy .
```

## Release Checklist

Before each release:

- [ ] All CI/CD checks are passing
- [ ] Documentation is up to date
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Version bump is appropriate (patch/minor/major)
- [ ] Working directory is clean
- [ ] You're on the main branch

After release:

- [ ] Verify the tag was created correctly
- [ ] Check that automated deployments triggered (if applicable)
- [ ] Update any dependent projects or documentation

## Troubleshooting

### Release script fails

If `release-patch.sh` fails:

1. Check that you're on the main branch: `git branch`
2. Ensure working tree is clean: `git status`
3. Verify you have push permissions
4. Check that `uv` is installed and working: `uv --version`

### Version conflicts

If there are version conflicts:

1. Check the current version: `uv version --short`
2. Verify the version in `pyproject.toml` matches expectations
3. Check for any uncommitted version changes