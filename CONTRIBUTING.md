# Contributing to zentity

## Feature requests and bug reports

Open an issue on the [issue list](https://github.com/zentity-io/zentity/issues).

## Development

zentity is developed in IntelliJ IDEA and uses Maven to manage dependencies, tests, and builds.

### Setting up the development environment

1. Install [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)  
2. Install [IntelliJ IDEA](https://www.jetbrains.com/idea/download/)  
3. Install `JDK 11`  
3.1 [Zulu/OpenJDK](https://www.azul.com/downloads/zulu-community/?package=jdk) (recommended)  
3.2 [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)  
4. Install [Docker](https://docs.docker.com/get-docker/)  
4.1 [Docker Compose](https://docs.docker.com/compose/install/) (optional)  

#### Create the project on IntelliJ IDEA

1. File > New > Project from Version Control
    - Version Control: Git
    - URL: https://www.github.com/zentity-io/zentity
    - Directory: Your local repository directory

#### Configure the project to use JDK 11

1. Navigate to File > Project Structure
2. Navigate to Project Settings > Project Settings
3. Set the Project SDK to the home path of JDK 11

#### Configure the Maven Run Configurations

1. Navigate to Run > Edit Configurations
2. Navigate to Maven
3. Create configurations with the following values for "Command line":
    - `clean install` - Runs all tests and builds zentity locally.
    - `clean install -e -X` - Runs all tests and builds zentity locally with extra debugging details.
    - `test -DskipIntegTests=true` - Runs unit tests locally.
4. Run these frequently to ensure that your tests continue to pass as you modify the code.

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

### Docker Compose

A [`docker-compose.yml`](docker-compose.yml) file is also provided to spin up an Elasticsearch and Kibana instance
with the locally-built plugin installed. This is helpful for playing around with local builds before publishing changes.

Ex:
```shell script
mvn clean package -DskipTests=true # build the plugin

docker-compose up # Run both Elasticsearch and Kibana
docker-compose down # Tear down Elasticsearch and Kibana

# Run Elasticsearch@7.10.0 with a debugger exposed on localhost port 5050
# and Kibana@7.10.0
export DEBUGGER_PORT=5050
export ELASTICSEARCH_VERSION=7.10.0
export ELASTICSEARCH_PORT=9200
export KIBANA_VERSION=7.10.0
export KIBANA_PORT=5601
docker-compose up
```

There is also a [`docker-compose.cluster.yml`](docker-compose.cluster.yml) that replicates a 3 node cluster with one
primary node and two data nodes. The available environment variables are identical to the single-node docker-compose file.


### Important files

- **pom.xml** - Maven configuration file that defines project settings, dependency versions, build behavior, and high level test behavior.
- **src/main/resources/plugin-descriptor.properties** - File required by all Elasticsearch plugins ([source](https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-authors.html#_plugin_descriptor_file)).
- **src/test/java** - Code for unit tests and integration tests. Unit tests are suffixed with `Test`. Integration test classes are suffixed with `IT`.
- **src/test/resources** - Data, entity models, and configurations used by tests.


### Software design choices

- **Input and outputs should mimic the experience of Elasticsearch.**
    This will make it easier for Elasticsearch users to work with the zentity. Here are some examples:
    - zentity and Elasticsearch both display results under `hits.hits`.
    - zentity and Elasticsearch use the `pretty` URL parameter to format JSON.

### Code formatting conventions

- Line separator: LF (`\n`)
- File encoding: UTF-8
- Indentation: 4 spaces
- Automatically reformat code with your IDE before committing. zentity uses the default reformatting configuration of IntelliJ IDEA.
- Break up large chunks of code into smaller chunks, and preface each code chunk with a brief comment that explains its purpose.

The [`.editorconfig`](.editorconfig) file configures editors for these and other code-style preferences.

### Submitting contributions

1. Create a branch.
2. Develop your changes.
3. Rebase your changes with the master branch.
4. Test your changes.
3. Submit a [pull request](https://github.com/fintechstudios/zentity/pulls). If your contribution addresses a feature or bug from the
[issues list](https://github.com/fintechstudios/zentity/issues), please link your pull request to the issue.

## Contacting the author

zentity is maintained by [davemoore-](https://github.com/davemoore-),
and the [@fintechstudios Team](https://github.com/fintechstudios),
who can help you with anything you need regarding this project.
