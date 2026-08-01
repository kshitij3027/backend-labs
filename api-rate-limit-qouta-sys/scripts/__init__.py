# Package marker only, and it exists from C1 for two concrete reasons rather than habit:
#
#   * Dockerfile.test runs `COPY scripts/ ./scripts/`, which fails the build if the directory is
#     absent — so scripts/ has to be non-empty from the first commit that has a tester image.
#   * The compose `e2e` and `loadtest` services run `python -m scripts.verify_e2e` /
#     `python -m scripts.load_test`, and `-m` only resolves a dotted name inside a package.
#
# The scripts themselves arrive in C13 and C14. They are excluded from coverage (see .coveragerc)
# because they are black-box harnesses that drive the app over HTTP from another container.
