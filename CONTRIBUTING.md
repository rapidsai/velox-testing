# Contributing to velox-testing

Contributions to velox-testing fall into the following categories:

1. To report a bug, request a new feature, or report a problem with documentation, please file an
   [issue](https://github.com/rapidsai/velox-testing/issues/new/choose) describing the problem or new feature
   in detail. The RAPIDS team evaluates and triages issues, and schedules them for a release. If you
   believe the issue needs priority attention, please comment on the issue to notify the team.
2. To propose and implement a new feature, please file a new feature request
   [issue](https://github.com/rapidsai/velox-testing/issues/new/choose). Describe the intended feature and
   discuss the design and implementation with the team and community. Once the team agrees that the
   plan looks good, go ahead and implement it, using the [code contributions](#code-contributions)
   guide below.
3. To implement a feature or bug fix for an existing issue, please follow the [code
   contributions](#code-contributions) guide below. If you need more context on a particular issue,
   please ask in a comment.

As contributors and maintainers to this project, you are expected to abide by velox-testing's code of
conduct. More information can be found at:
[Contributor Code of Conduct](https://docs.rapids.ai/resources/conduct/).

## Code contributions

1. Find an issue to work on. The best way is to look for the
   [good first issue](https://github.com/rapidsai/velox-testing/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
   or [help wanted](https://github.com/rapidsai/velox-testing/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
   labels.
2. Comment on the issue stating that you are going to work on it.
3. Create a fork of the velox-testing repository and check out a branch with a name that
   describes your planned work. For example, ix-documentation
4. Write code to address the issue or implement the feature.
5. Add unit tests and unit benchmarks.
6. [Create your pull request](https://github.com/rapidsai/velox-testing/compare). To run continuous integration (CI) tests without requesting review, open a draft pull request.
7. Verify that CI passes all [status checks](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/about-status-checks).
   Fix if needed.  TODO: link to appropriate CI once it has stabilized.
8. Wait for other developers to review your code and update code as needed.
9. Once reviewed and approved, a RAPIDS developer will merge your pull request.

If you are unsure about anything, don't hesitate to comment on issues and ask for clarification!