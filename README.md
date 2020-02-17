# Flink-sql-gateway

Flink-sql-gateway is a service that allows other applications to easily interact with a Flink cluster through a REST API. 

User applications (e.g. Java/Python/Shell program, Postman) can use the REST API to submit queries, cancel jobs, retrieve results, etc. 

[flink-jdbc-driver](https://github.com/ververica/flink-jdbc-driver) enables JDBC clients to connect to flink-sql-gateway based on the REST API.

Currently, the REST API is internal API and we recommend users to interact with the gateway through JDBC API. 
Flink-sql-gateway stores the session properties in memory now. If the service is stopped or crashed, all properties are lost. We will improve this in future.


# Startup gateway

There are four steps to starting the service from scratch:

1. Downloads (or builds) the Flink package. Flink-sql-gateway only supports Apache Flink-1.10 now, you can download it through https://flink.apache.org/downloads.html#apache-flink-1100.

2. Starts up a Flink cluster. Flink-sql-gateway requires a running Flink cluster where table programs can be executed. For more information about setting up a Flink cluster see the [Cluster & Deployment](https://ci.apache.org/projects/flink/flink-docs-release-1.10/ops/deployment/cluster_setup.html) part.

3. Configures the FLINK_HOME environment variable using the following command (and add the same into ~/.bashrc): export FLINK_HOME=<flink-install-dir>

4. Downloads (or builds) the flink-sql-gateway package and executes `./bin/start-gateway.sh`

The gateway can be started with the following optional CLI commands.
```
./bin/start-gateway.sh -h

The following options are available:
     -d,--defaults <default configuration file>   The properties with which every new session is initialized. 
                                                  Properties might be overwritten by session properties.
     -h,--help                                    Show the help message with descriptions of all options.
     -j,--jar <JAR file>                          A JAR file to be imported into the session. 
                                                  The file might contain user-defined classes needed for 
                                                  statements such as functions, the execution of table sources,
                                                  or sinks. Can be used multiple times.
     -l,--library <JAR directory>                 A JAR file directory with which every new session is initialized. 
                                                  The files might contain user-defined classes needed for 
                                                  the execution of statements such as functions,
                                                  table sources, or sinks. Can be used multiple times.
     -p,--port <service port>                     The port to which the REST client connects to.
```

If no configuration file is specified, the gateway will read its default configuration from the file located in ./conf/sql-gateway-defaults.yaml. See the [configuration](#Configuration) part for more information about the structure of environment files. 
If no port is specified in CLI commands or in the configuration file, the gateway will be started with the default port 8083. 


# Configuration

A SQL query needs a configuration environment in which it is executed. The so-called environment file defines server properties, session properties, available catalogs, table sources and sinks, user-defined functions, and other properties required for execution and deployment.

Every environment file is a regular [YAML file](http://yaml.org/). An example of such a file is presented below.

```
# Define server properties.

server:
  bind-address: 127.0.0.1           # optional: The address that the gateway binds itself (127.0.0.1 by default)
  address: 127.0.0.1                # optional: The address that should be used by clients to connect to the gateway (127.0.0.1 by default)
  port: 8083                        # optional: The port that the client connects to  (8083 by default)


# Define session properties.

session:
  idle-timeout: 1d                  # optional: Session will be closed when it's not accessed for this duration, which can be disabled by setting to zero. The minimum unit is in milliseconds. (1d by default)
  check-interval: 1h                # optional: The check interval for session idle timeout, which can be disabled by setting to zero. The minimum unit is in milliseconds. (1h by default)
  max-count: 1000000                # optional: Max count of active sessions, which can be disabled by setting to zero. (1000000 by default)


# Define tables here such as sources, sinks, views, or temporal tables.

tables:
  - name: MyTableSource
    type: source-table
    update-mode: append
    connector:
      type: filesystem
      path: "/path/to/something.csv"
    format:
      type: csv
      fields:
        - name: MyField1
          type: INT
        - name: MyField2
          type: VARCHAR
      line-delimiter: "\n"
      comment-prefix: "#"
    schema:
      - name: MyField1
        type: INT
      - name: MyField2
        type: VARCHAR
  - name: MyCustomView
    type: view
    query: "SELECT MyField2 FROM MyTableSource"

# Define user-defined functions here.

functions:
  - name: myUDF
    from: class
    class: foo.bar.AggregateUDF
    constructor:
      - 7.6
      - false

# Define available catalogs

catalogs:
   - name: catalog_1
     type: hive
     property-version: 1
     hive-conf-dir: ...
   - name: catalog_2
     type: hive
     property-version: 1
     default-database: mydb2
     hive-conf-dir: ...
     hive-version: 1.2.1


# Properties that change the fundamental execution behavior of a table program.

execution:
  parallelism: 1                    # optional: Flink's parallelism (1 by default)
  max-parallelism: 16               # optional: Flink's maximum parallelism (128 by default)
  current-catalog: catalog_1        # optional: name of the current catalog of the session ('default_catalog' by default)
  current-database: mydb1           # optional: name of the current database of the current catalog
                                    #   (default database of the current catalog by default)


# Configuration options for adjusting and tuning table programs.

# A full list of options and their default values can be found
# on the dedicated "Configuration" page.
configuration:
  table.optimizer.join-reorder-enabled: true
  table.exec.spill-compression.enabled: true
  table.exec.spill-compression.block-size: 128kb

# Properties that describe the cluster to which table programs are submitted to.

deployment:
  response-timeout: 5000
```

This configuration:
- defines the server startup options that it will be started with bind-address `127.0.0.1`, address `127.0.0.1` and port `8083`,
- defines the session's limitations that a session will be closed when it's not accessed for `1d` (idle-timeout), the server checks whether the session is idle timeout for every `1h` (check-interval), and only `1000000` (max-count ) active sessions is allowed.
- defines an environment with a table source `MyTableSource` that reads from a CSV file,
- defines a view `MyCustomView` that declares a virtual table using a SQL query,
- defines a user-defined function `myUDF` that can be instantiated using the class name and two constructor parameters,
- connects to two Hive catalogs and uses `catalog_1` as the current catalog with `mydb1` as the current database of the catalog,
- the default parallelism is 1, and the max parallelism is 16,
- and makes some planner adjustments around join reordering and spilling via configuration options.

Properties that have been set (using the `SET` command) within a session have highest precedence.


# Dependencies

The gateway does not require to setup a Java project using Maven or SBT. Instead, you can pass the dependencies as regular JAR files that get submitted to the cluster. You can either specify each JAR file separately (using `--jar`) or define entire library directories (using `--library`). For connectors to external systems (such as Apache Kafka) and corresponding data formats (such as JSON), Flink provides **ready-to-use JAR bundles**. These JAR files can be downloaded for each release from the Maven central repository.

The full list of offered SQL JARs and documentation about how to use them can be found on the [connection to external systems page](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html).

# Supported statements

The following statements are supported now.
|  statement   | comment  |
|  ----  | ----  |
| SHOW_CATALOGS  | Lists all registered catalogs |
| SHOW_CURRENT_CATALOG  | Shows current catalog |
| SHOW_DATABASES  | Lists all databases in the current catalog |
| SHOW_CURRENT_DATABASE  | Shows current database in current catalog |
| SHOW_TABLES  | Lists all tables in the current database of the current catalog |
| SHOW_FUNCTIONS  | Lists all functions |
| SHOW_MODULES  | Lists all modules |
| USE_CATALOG catalog_name | Sets a catalog with given name as the current catalog |
| USE database_name | Sets a database with given name as the current database of the current catalog |
| CREATE TABLE table_name ... | Creates a table with a DDL statement |
| DROP TABLE table_name | Drops a table with given name |
| ALTER TABLE table_name | Alters a table with given name |
| CREATE DATABASE database_name ... | Creates a database in current catalog with given name |
| DROP DATABASE database_name ... | Drops a database with given name |
| ALTER DATABASE database_name ... | Alters a database with given name |
| CREATE VIEW view_name AS ... | Adds a view in current session with SELECT statement |
| DROP VIEW view_name ... | Drops a table with given name |
| SET xx=yy | Sets given key's session property to the specific value |
| SET | Lists all session's properties |
| RESET | Resets all session's properties set by `SET` command |
| DESCRIBE table_name | Shows the schema of a table |
| EXPLAIN ... | Shows string-based explanation about AST and execution plan of the given statement |
| SELECT ... | Submits a Flink `SELECT` SQL job |
| INSERT INTO ... | Submits a Flink `INSERT INTO` SQL job |
| INSERT OVERWRITE ... | Submits a Flink `INSERT OVERWRITE` SQL job |



