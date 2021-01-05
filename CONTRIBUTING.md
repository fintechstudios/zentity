# Contributing to zentity


## Feature requests and bug reports

Open an issue on the [issue list](https://github.com/zentity-io/zentity/issues).


## Development

zentity was developed in IntelliJ and uses Maven to manage dependencies, tests, and builds.

Indents are four spaces instead of tabs. Other code-style preferences are configured in
the [`.editorconfig`](.editorconfig). Auto-reformatting code from your IDE is encouraged.

### Integration Tests

The project uses [testcontainers](https://testcontainers.org/) to spin up a local Docker Elasticsearch cluster
for testing. This requires the Docker daemon to be present and running. 

Integration tests can be run via Maven or through the IDE. If running outside of Maven, you must specify the following
environment variables:

* `ELASTICSEARCH_VERSION` - The version of Elasticsearch to test against.
* `PLUGIN_BUILD_DIR` - The directory where the compiled plugin lives, which should be `${PROJECT ROOT}/target/releases`. 

#### Debugging

Since the plugin needs to be installed on the Elasticsearch cluster for integration tests, using a debugger can be tricky
but is possible. There are a few environment variables that will configure and expose a remote debugger
on the Elasticsearch container that can be attached to, for example via IntelliJ.

* `DEBUGGER_ENABLED` - Whether to enable the debugger, default: `false`.
* `DEBUGGER_PORT` - What local port to expose the debugger on, default: a random port.
* `DEBUGGER_SLEEP` - Duration to sleep in milliseconds for after starting the debugger,
so it can be attached to before tests run, default: `5000`.

## Submitting contributions

1. Test your changes.

2. Rebase your changes with the master branch.

3. Submit a [pull request](https://github.com/fintechstudios/zentity/pulls). If your contribution addresses a feature or bug from the
[issues list](https://github.com/fintechstudios/zentity/issues), please link your pull request to the issue.


## Contacting the author

zentity is maintained by [davemoore-](https://github.com/davemoore-),
and the [@fintechstudios Team](https://github.com/fintechstudios),
who can help you with anything you need regarding this project.
