# ODPS Console

[![Build Status](https://travis-ci.org/aliyun/aliyun-odps-console.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-odps-console)

The ODPS Console is a command-line interface for interacting with Alibaba Cloud's MaxCompute (
formerly ODPS) big data processing platform. It provides a powerful and flexible way to manage and
interact with MaxCompute projects, tables, resources, and jobs directly from the terminal.

MaxCompute is a fully managed, serverless data warehousing and analytics platform that allows you to
process exabytes of data with high concurrency and throughput. The ODPS Console enables developers,
data engineers, and analysts to perform common tasks such as running SQL queries,
uploading/downloading data, managing resources, and submitting MapReduce jobs.

## Features

The ODPS Console offers a comprehensive set of features organized in a modular plugin architecture:

* **SQL Execution** - Run interactive SQL queries and scripts against MaxCompute tables with support
  for:
    * Standard SQL syntax with extensions
    * Interactive query mode for faster execution
    * Query cost estimation and confirmation for large data operations
    * Result set visualization and export
    * Support for materialized views, virtual views, and external tables

* **Data Tunnel** - High-performance data upload and download between local files and MaxCompute
  tables with:
    * Support for various file formats (CSV, JSON, etc.)
    * Parallel data transfer for improved performance
    * Data size confirmation to prevent accidental large transfers
    * Custom tunnel endpoints for optimized data transfer

* **MapReduce Jobs** - Submit and manage MapReduce jobs on the MaxCompute platform:
    * Execute distributed computing tasks
    * Monitor job progress and status
    * Manage job configurations and parameters

* **Resource Management** - Create, update, and delete resources used by MaxCompute functions:
    * Support for JAR, Python, and other resource types
    * Function creation and management with class path specification
    * Resource dependency management for complex functions

* **Workflow Management** - Execute and monitor XFlow workflows:
    * Manage offline and online machine learning models
    * Execute complex data processing pipelines
    * Monitor workflow instances and status

* **Authentication** - Secure authentication with multiple credential options:
    * AccessKey/SecretKey authentication
    * STS token authentication for temporary credentials
    * Application-based authentication
    * Support for various account providers (aliyun, sts, etc.)

* **Volume File System** - Manage files in the MaxCompute volume file system:
    * Create and manage external volumes
    * File operations within the MaxCompute environment
    * Integration with Hadoop-compatible file system operations

* **Cupid Integration** - Support for managed service features:
    * Spark job submission and management
    * Job monitoring and visualization
    * Integration with Alibaba Cloud managed services

* **Advanced Table Operations** - Comprehensive table management capabilities:
    * Show tables with filtering and pattern matching
    * Detailed table schema inspection with extended metadata
    * Partition management for partitioned tables
    * Table lifecycle and storage management

* **Instance Management** - Complete control over job instances:
    * Submit, monitor, and manage SQL and other job instances
    * Instance priority settings for resource allocation
    * Wait and status checking commands
    * Instance termination capabilities

* **Project Management** - Multi-project support with:
    * Project switching and context management
    * Project-level metadata inspection
    * Cross-project resource access

* **Interactive Mode** - Enhanced command-line experience:
    * Persistent session with command history
    * Tab completion for commands and table names
    * Syntax highlighting and formatting
    * Multi-line command editing

## Getting Started

### Prerequisites

* Java 8 or higher
* An Alibaba Cloud account with MaxCompute service enabled
* Network access to MaxCompute service endpoints

### Installation

#### Option 1: Download Pre-built Package (Recommended)

1. Download the latest release:
   ```bash
   wget https://github.com/aliyun/aliyun-odps-console/releases/latest/download/odpscmd_public.zip
   ```

   If the link above doesn't work, try:
   ```bash
   wget https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/odpscmd/latest/odpscmd_public.zip
   ```

2. Extract the package:
   ```bash
   unzip odpscmd_public.zip
   cd odpscmd_public
   ```

3. Verify the installation by checking the directory structure:
   ```
   bin/          # Executable scripts
   conf/         # Configuration files
   lib/          # Library dependencies
   plugins/      # Plugin modules
   ```

#### Option 2: Build from Source

1. Ensure you have the prerequisites installed:
    * Java 8 or higher
    * Apache Maven 3.0 or higher

2. Clone the repository and build:
   ```bash
   git clone https://github.com/aliyun/aliyun-odps-console.git
   cd aliyun-odps-console
   mvn clean package -DskipTests
   ```

3. The built package will be available at:
   ```
   odps-console-dist-public/target/odpscmd_public.zip
   ```

### Configuration

Before using the ODPS Console, you need to configure it with your MaxCompute credentials and
settings.

1. Create a configuration file at `conf/odps_config.ini`:
   ```ini
   # Basic configuration (required)
   project_name = your_project_name
   access_id = your_access_id
   access_key = your_access_key
   end_point = your_endpoint
   
   # Optional configurations for enhanced functionality
   log_view_host = https://logview.odps.aliyun.com
   debug = false
   instance_priority = 9
   ```

2. You can obtain your endpoint from
   the [MaxCompute endpoints documentation](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints).

3. For detailed configuration options, see the [Configuration section](#configuration) below.

### Quick Start

1. Run the console in interactive mode:
   ```bash
   ./bin/odpscmd
   ```

2. You should see a prompt like:
   ```
   ODPS Console
   Aliyun ODPS Command Line Tool
   Version 0.52.3-public
   
   odps@your_project_name>
   ```

3. Execute a simple command to verify your setup:
   ```sql
   odps@your_project_name> show tables;
   ```

4. For batch execution, you can run commands directly:
   ```bash
   # Execute a single SQL command
   ./bin/odpscmd -e "show tables;"
   
   # Execute a SQL script file
   ./bin/odpscmd -f /path/to/script.sql
   ```

### Common First Steps

1. **List tables in your project:**
   ```sql
   show tables;
   ```

2. **Describe a table structure:**
   ```sql
   desc table_name;
   ```

3. **Run a simple query:**
   ```sql
   select * from table_name limit 10;
   ```

4. **Upload data to a table:**
   ```sql
   tunnel upload /path/to/local/file.txt table_name;
   ```

5. **Download data from a table:**
   ```sql
   tunnel download table_name /path/to/local/file.txt;
   ```

### Command Line Options

The ODPS Console supports several command-line options:

* `-e "command"` - Execute a single command and exit
* `-f script.sql` - Execute commands from a script file
* `--config file` - Specify a custom configuration file
* `-h, --help` - Display help information
* `-v, --version` - Display version information

Example:

```bash
./bin/odpscmd -e "select count(*) from my_table;" --config /path/to/custom_config.ini
```

## Configuration

The ODPS Console requires configuration to connect to your MaxCompute project. The configuration is
specified in an `odps_config.ini` file with the following parameters:

### Basic Configuration Parameters

| Parameter      | Description                             | Required | Default Value |
|----------------|-----------------------------------------|----------|---------------|
| `project_name` | The name of your MaxCompute project     | Yes      | None          |
| `access_id`    | Your Alibaba Cloud Access Key ID        | Yes      | None          |
| `access_key`   | Your Alibaba Cloud Access Key Secret    | Yes      | None          |
| `end_point`    | The API endpoint for MaxCompute service | Yes      | None          |
| `schema_name`  | The schema name in MaxCompute           | No       | None          |

### Network and Proxy Configuration

| Parameter                 | Description                                | Required | Default Value |
|---------------------------|--------------------------------------------|----------|---------------|
| `proxy_host`              | Proxy server hostname                      | No       | None          |
| `proxy_port`              | Proxy server port                          | No       | None          |
| `https_check`             | Enable SSL certificate verification        | No       | true          |
| `network_read_timeout`    | Network read timeout in milliseconds       | No       | None          |
| `network_connect_timeout` | Network connection timeout in milliseconds | No       | None          |

### Logging and Debugging Configuration

| Parameter          | Description                               | Required | Default Value                   |
|--------------------|-------------------------------------------|----------|---------------------------------|
| `log_view_host`    | The host for LogView (job execution logs) | No       | https://logview.odps.aliyun.com |
| `log_view_version` | LogView version                           | No       | None                            |
| `log_view_life`    | LogView life time in hours                | No       | 720 (30 days)                   |
| `debug`            | Enable debug mode for detailed logging    | No       | false                           |

### Authentication Configuration

| Parameter          | Description                                            | Required | Default Value |
|--------------------|--------------------------------------------------------|----------|---------------|
| `account_provider` | Authentication method (aliyun, sts, etc.)              | No       | aliyun        |
| `sts_token`        | Security Token Service token for temporary credentials | No       | None          |

### Job and Execution Configuration

| Parameter                    | Description                                          | Required | Default Value |
|------------------------------|------------------------------------------------------|----------|---------------|
| `instance_priority`          | Priority for job execution (0-9, where 0 is highest) | No       | 9             |
| `use_instance_tunnel`        | Use instance tunnel for SQL result download          | No       | false         |
| `instance_tunnel_max_record` | Maximum records to download via instance tunnel      | No       | None          |
| `instance_tunnel_max_size`   | Maximum size to download via instance tunnel (bytes) | No       | None          |
| `running_cluster`            | Specify the cluster to run jobs                      | No       | None          |

### Interactive Mode Configuration

| Parameter                       | Description                                           | Required | Default Value  |
|---------------------------------|-------------------------------------------------------|----------|----------------|
| `enable_interactive_mode`       | Enable interactive query mode                         | No       | false          |
| `interactive_service_name`      | Name of the interactive service                       | No       | public.default |
| `interactive_output_compatible` | Enable compatible output mode for interactive queries | No       | false          |
| `interactive_auto_rerun`        | Automatically rerun failed interactive queries        | No       | false          |
| `interactive_max_attach`        | Maximum number of sessions to attach                  | No       | None           |
| `attach_session_timeout`        | Timeout for attaching sessions (seconds)              | No       | 60             |
| `keep_session_variables`        | Keep session variables when switching projects        | No       | false          |

### Tunnel and Data Transfer Configuration

| Parameter           | Description                               | Required | Default Value |
|---------------------|-------------------------------------------|----------|---------------|
| `tunnel_endpoint`   | Custom endpoint for tunnel service        | No       | None          |
| `hub_endpoint`      | Custom endpoint for DataHub service       | No       | None          |
| `data_size_confirm` | Confirmation threshold for data size (GB) | No       | None          |

### Performance and Optimization Configuration

| Parameter            | Description                                    | Required | Default Value |
|----------------------|------------------------------------------------|----------|---------------|
| `lite_mode`          | Enable lite mode for faster execution          | No       | false         |
| `enable_quota_cache` | Enable quota information caching               | No       | false         |
| `skip_progress`      | Skip progress reporting for better performance | No       | false         |

### System and Update Configuration

| Parameter         | Description                           | Required | Default Value |
|-------------------|---------------------------------------|----------|---------------|
| `update_url`      | Custom URL for update checks          | No       | None          |
| `post_hook_class` | Custom post-hook class for extensions | No       | None          |
| `user_commands`   | Custom user commands configuration    | No       | None          |

### Session and Fallback Configuration

| Parameter              | Description                              | Required | Default Value |
|------------------------|------------------------------------------|----------|---------------|
| `fallback.resource`    | Fallback policy for resource not enough  | No       | None          |
| `fallback.unsupported` | Fallback policy for unsupported features | No       | None          |
| `fallback.timeout`     | Fallback policy for query timeout        | No       | None          |
| `fallback.upgrading`   | Fallback policy for service upgrading    | No       | None          |
| `fallback.attach`      | Fallback policy for attach failure       | No       | None          |
| `fallback.unknown`     | Fallback policy for unknown errors       | No       | None          |

### Additional Configuration Options

You can also set any MaxCompute system properties using the `set.` prefix. For example:

- `set.odps.sql.timezone=UTC` - Set the timezone for SQL operations
- `set.odps.namespace.schema=true` - Enable schema namespace mode
- `set.odps.task.wlm.quota=your_quota_name` - Set the WLM quota for tasks

### Configuration File Locations

The console looks for configuration files in the following order:

1. Path specified by the `--config` command line option
2. `./odps_config.ini` in the current directory
3. `~/odps_config.ini` in the user's home directory
4. `conf/odps_config.ini` in the installation directory

## Usage

### Interactive Mode

Run the console without arguments to enter interactive mode:

```bash
./bin/odpscmd
```

In interactive mode, you can execute commands directly:

```sql
odps
@
your_project_name
> show tables;
odps@your_project_name
>
select *
from my_table limit 10;
odps
@
your_project_name
>
drop table my_table;
```

Interactive mode features:

* Command history navigation with up/down arrow keys
* Tab completion for commands, table names, and column names
* Multi-line command editing
* Syntax highlighting for SQL commands
* Persistent session with automatic reconnection

### Batch Mode

Execute a single command or script:

```bash
# Execute a single SQL command
./bin/odpscmd -e "show tables;"

# Execute a SQL script file
./bin/odpscmd -f /path/to/script.sql

# Execute with custom configuration
./bin/odpscmd --config /path/to/custom_config.ini -e "select count(*) from my_table;"
```

### Common Commands

#### Table Management

```sql
# List all tables in the current project
show tables;

# List tables with a specific prefix
show tables like 'my_prefix%';

# Show external tables only
show external tables;

# Describe table structure
desc my_table;

# Describe table with extended information
desc extended my_table;

# Show table partitions
show partitions my_table;

# Show table partitions with filter
show partitions my_table partition(region='us-west-1');
```

#### Data Operations

```sql
# Preview table data
select *
from my_table limit 10;

# Count rows in a table
select count(*)
from my_table;

# Create a new table
create table new_table
(
    id   bigint,
    name string,
    age  int
);

# Insert data into a table
insert into table new_table values (1, 'John', 25);

# Drop a table
drop table my_table;
```

#### Data Tunnel Operations

```sql
# Upload data from a local file to a table
tunnel upload /path/to/local/data.csv my_table;

# Upload with specific field delimiter
tunnel upload -fd '|' /path/to/data.txt my_table;

# Download data from a table to a local file
tunnel download my_table /path/to/local/data.csv;

# Download with specific charset
tunnel download -c utf-8 my_table /path/to/data.txt;

# Resume an interrupted upload
tunnel resume session_id;
```

#### Project Management

```sql
# Switch to a different project
use another_project;

# Show current project information
whoami;

# List all accessible projects
list projects;
```

#### Instance Management

```sql
# Show running instances
show instances;

# Show specific instance status
show instance instance_id;

# Wait for an instance to complete
wait instance_id;

# Stop a running instance
stop instance_id;

# Get detailed instance status
status instance_id;
```

#### Resource Management

```sql
# Add a JAR file as a resource
add jar /path/to/my_udf.jar;

# Add a Python file as a resource
add py /path/to/my_script.py;

# List all resources
list resources;

# Describe a resource
desc resource my_resource;

# Remove a resource
remove resource my_resource;
```

#### Function Management

```sql
# Create a function from a resource
create function my_func as 'com.example.MyUDF' using 'my_udf.jar';

# List all functions
list functions;

# Describe a function
desc function my_func;

# Drop a function
drop function my_func;
```

#### Configuration Commands

```sql
# Set a configuration parameter for the current session
set odps.sql.timezone=UTC;

# View current configuration
show flags;
```

## Building from Source

### Prerequisites

* Java 8 or higher
* Apache Maven 3.0 or higher

### Build Process

1. Clone the repository:
   ```bash
   git clone https://github.com/aliyun/aliyun-odps-console.git
   cd aliyun-odps-console
   ```

2. Build the project:
   ```bash
   # Build without running tests
   mvn clean package -DskipTests
   
   # Build with tests
   mvn clean package
   ```

3. The built package will be available at:
   ```
   odps-console-dist-public/target/odpscmd_public.zip
   ```

### Running Tests

Before running tests, you need to configure ODPS configuration files in these locations:

* `odps-console-tunnel/src/test/resources/odps_config.ini`
* `odps-console-dist-public/odps_config.ini`
* `odps-console-public/src/test/resources/odps_config.ini`
* `odps-console-mr/src/test/resources/odps_config.ini`
* `odps-console-sql/src/test/resources/odps_config.ini`
* `odps-console-xflow/src/test/resources/odps_config.ini`
* `odps-console-auth/src/test/resources/odps_config.ini`
* `odps-console-basic/src/test/resources/odps_config.ini`
* `odps-console-dship/src/test/resources/odps_config.ini`
* `odps-console-resource/src/test/resources/odps_config.ini`

Run all tests:

```bash
mvn clean test
```

## Architecture

The ODPS Console follows a modular plugin architecture built with Apache Maven, allowing for
extensibility and maintainability. Each module serves a specific purpose and can be developed,
tested, and deployed independently.

### Core Modules

* **odps-console-basic**: Core console functionality and main entry point
    * Command parsing and execution framework
    * Configuration management and context handling
    * Basic utility functions and common services
    * Main entry point: `com.aliyun.openservices.odps.console.ODPSConsole`

* **odps-console-public**: Public command implementations
    * Standard SQL commands (SELECT, INSERT, CREATE, DROP, etc.)
    * Table management commands (SHOW, DESCRIBE, etc.)
    * Instance management commands (WAIT, STOP, STATUS, etc.)
    * Project management commands (USE, WHOAMI, LIST PROJECTS, etc.)

* **odps-console-sql**: SQL execution commands
    * Advanced SQL query execution
    * Interactive query mode support
    * Query cost estimation and optimization
    * Result set processing and formatting

* **odps-console-dship**: Data upload/download tunnel commands
    * High-performance data transfer between local files and MaxCompute tables
    * Support for various file formats (CSV, JSON, etc.)
    * Parallel data transfer for improved performance
    * Session resumption for interrupted transfers

* **odps-console-mr**: MapReduce job submission commands
    * MapReduce job creation and execution
    * Job configuration and parameter management
    * Progress monitoring and status reporting

* **odps-console-resource**: Resource management commands
    * Resource creation, update, and deletion (JAR, Python, file resources)
    * Function management (CREATE, DROP, LIST, DESCRIBE functions)
    * Resource dependency tracking

* **odps-console-xflow**: XFlow workflow commands
    * Workflow execution and monitoring
    * Offline and online model management
    * Machine learning pipeline execution

* **odps-console-auth**: Authentication commands
    * Multiple authentication provider support (aliyun, sts, etc.)
    * Security policy management
    * Access control and authorization

* **odps-console-volume2**: Volume file system commands
    * External volume management
    * File operations within the MaxCompute environment
    * Integration with Hadoop-compatible file systems

* **odps-console-cupid-public**: Cupid commands
    * Spark job submission and management
    * Managed service integration
    * Job monitoring and visualization

* **odps-console-dist-public**: Distribution packaging module
    * Packaging and distribution of the complete console
    * Assembly configuration for release builds
    * Dependency management for the final package

### Deprecated Modules

* **odps-console-tunnel**: Legacy data tunnel commands (deprecated)
    * Replaced by odps-console-dship for better performance and features

### Command Processing Flow

1. **Entry Point**: The main class `com.aliyun.openservices.odps.console.ODPSConsole` receives
   command-line arguments
2. **Configuration Loading**: Execution context is initialized with configuration from files or
   command-line options
3. **Command Parsing**: Input is parsed to identify the appropriate command handler
4. **Plugin Loading**: Required plugins are dynamically loaded based on command type
5. **Command Execution**: The specific command implementation is executed with the current context
6. **Result Output**: Results are formatted and displayed to the user

### Extensibility

The modular architecture allows for easy extension:

* New commands can be added by implementing the `AbstractCommand` class
* Custom plugins can be developed and integrated
* Configuration can be extended through the `odps_config.ini` file
* Post-execution hooks can be added for custom processing

## Contributing

We welcome contributions to the ODPS Console project! Here's how you can help:

### Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/your-username/aliyun-odps-console.git
   ```
3. Create a new branch for your feature or bug fix:
   ```bash
   git checkout -b feature/AmazingFeature
   ```

### Development Setup

1. **Prerequisites**:
    * Java 8 or higher
    * Apache Maven 3.0 or higher
    * An Alibaba Cloud account with MaxCompute service enabled (for testing)

2. **Build the project**:
   ```bash
   cd aliyun-odps-console
   mvn clean package -DskipTests
   ```

3. **Configure test environment**:
   Before running tests, you need to configure ODPS configuration files in these locations:
    * `odps-console-tunnel/src/test/resources/odps_config.ini`
    * `odps-console-dist-public/odps_config.ini`
    * `odps-console-public/src/test/resources/odps_config.ini`
    * `odps-console-mr/src/test/resources/odps_config.ini`
    * `odps-console-sql/src/test/resources/odps_config.ini`
    * `odps-console-xflow/src/test/resources/odps_config.ini`
    * `odps-console-auth/src/test/resources/odps_config.ini`
    * `odps-console-basic/src/test/resources/odps_config.ini`
    * `odps-console-dship/src/test/resources/odps_config.ini`
    * `odps-console-resource/src/test/resources/odps_config.ini`

4. **Run tests**:
   ```bash
   mvn clean test
   ```

### Code Standards

* **Code Style**: Follow the existing code style and conventions used in the project
* **Documentation**: Update README.md and other documentation when adding new features
* **Testing**: Include unit tests for new functionality and ensure all tests pass
* **Commit Messages**: Write clear, descriptive commit messages following conventional commit format
* **Branch Naming**: Use descriptive branch names (e.g., `feature/user-authentication`,
  `bugfix/sql-parser-error`)

### Development Workflow

1. **Issue Tracking**: Check existing issues or create a new one to discuss your proposed changes
2. **Implementation**: Make your changes in a feature branch
3. **Testing**: Ensure all existing tests pass and add new tests for your changes
4. **Documentation**: Update relevant documentation to reflect your changes
5. **Code Review**: Submit a pull request for review by the maintainers

### Module-Specific Development

The ODPS Console uses a modular architecture. When contributing:

* **New Commands**: Implement new commands by extending the `AbstractCommand` class
* **SQL Extensions**: Add SQL functionality in the `odps-console-sql` module
* **Data Transfer**: Enhance data tunnel capabilities in the `odps-console-dship` module
* **Authentication**: Add authentication methods in the `odps-console-auth` module
* **Resource Management**: Extend resource handling in the `odps-console-resource` module

### Testing Guidelines

* **Unit Tests**: Write unit tests for new functionality using JUnit
* **Integration Tests**: Include integration tests for complex features
* **Edge Cases**: Test edge cases and error conditions
* **Performance**: Consider performance implications of your changes

### Pull Request Process

1. Ensure your code follows the project's coding standards
2. Include tests that cover your changes
3. Update documentation to reflect any changes in functionality
4. Submit a pull request with a clear description of the changes
5. Address any feedback from reviewers promptly

### Reporting Issues

If you find a bug or have a feature request:

1. Check if there's already an issue for it
2. If not, create a new issue with:
    * A clear, descriptive title
    * Detailed steps to reproduce (for bugs)
    * Expected and actual behavior
    * Environment information (OS, Java version, etc.)
    * Any relevant logs or error messages

### Community

* Be respectful and constructive in all interactions
* Help others by answering questions and reviewing pull requests
* Participate in discussions about the project's direction and features

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)