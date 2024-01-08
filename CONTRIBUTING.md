# Contributing to Graphen_J

Thank you for your interest in contributing to Graphen_J! We welcome contributions and improvements from the community.
This document will guide you through the contribution process.

## Reporting Bugs

If you have found a bug in the project, please report it on our issue tracking system
at [Github Issues](https://github.com/jobtech-dev/Graphen_J/issues). Make sure to include detailed information about the
issue, such as the operating system, Scala version, Spark, Deequ, Iceberg, and any other relevant libraries or
dependencies. Additionally, if possible, provide a reproducible example or code snippet.

## Proposing New Features or Enhancements

If you have ideas for new features or enhancements to the project, we encourage you to discuss them on our issue
tracking system at [Github Issues](https://github.com/jobtech-dev/Graphen_J/issues) before starting to work on them.
This allows us to evaluate the importance and feasibility of the proposals and ensure there are no duplications of
effort.

## Contributing Code

If you wish to contribute code to Graphen_J, follow these steps:

1. Make sure you have a Github account.
2. Fork the repository [jobtech-dev/Graphen_J](https://github.com/jobtech-dev/Graphen_J) to your Github account.
3. Clone your fork to your local machine.
4. From the project root directory run the command `./build.sh` to build the project
5. Create a new branch for your work: `git checkout -b feature/branch-name` (for new features)
   or `git checkout -b fix/branch-name` (for bug fixes).
6. Implement the changes or add new features.
7. Ensure you write appropriate tests for your changes.
8. Run all existing tests and ensure they pass without errors.
9. Run the linter and address any warnings or errors.
10. Commit your changes with a descriptive message following this structure:

```
[#IssueNumber] Issue Title
- added ...
- updated ...
- fixed ...

```

Replace `#IssueNumber` with the actual issue number you're working on and provide a concise and descriptive commit
message. List the changes or additions in bullet points following the commit message. Include `PR #IssueNumber` at the
end of the message to link it to the related pull request.

10. Squash and rebase your branch onto the target branch before it can be merged.
11. Push your branch to your Github repository: `git push origin branch-name`.
12. Submit a pull request to the main repository [jobtech-dev/Graphen_J](https://github.com/jobtech-dev/Graphen_J).
13. Be prepared to address any comments or feedback on your pull request.

## Code Style Guidelines

To maintain consistency and readability in the codebase, please follow these guidelines:

- Use standard Scala formatting.
- Write readable code with descriptive variable and function names.
- Add relevant comments to explain complex parts of the code.
- Write appropriate tests to cover the code you are contributing.
- Follow the naming conventions for classes, methods, and variables used in the project.

## Useful Resources

- Spark Documentation: [link to Spark documentation](https://spark.apache.org/docs/latest/)
- Deequ Documentation: [link to Deequ documentation](https://github.com/awslabs/deequ)
- Iceberg Documentation: [link to Iceberg documentation](https://iceberg.apache.org/)

## Contact

If you have any questions or need further assistance, feel free to contact us via email.

Thank you again for your contribution!

