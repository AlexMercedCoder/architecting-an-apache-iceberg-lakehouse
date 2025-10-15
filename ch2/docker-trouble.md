# Docker Troubleshooting Guide for Chapter 2

This guide helps you resolve common issues when setting up and running the Docker-based development environment introduced in Chapter 2 of *Architecting an Apache Iceberg Lakehouse*. Whether you're working through the examples for the first time or encountering setup errors, this guide provides actionable troubleshooting advice.

## Overview

The Docker environment for Chapter 2 is designed to streamline local experimentation with Apache Iceberg using containerized services. It includes all necessary components pre-configured to simulate a basic lakehouse environment.

**Purpose of the Docker setup:**
- Provide a reproducible, isolated environment for running PySpark jobs with Iceberg.
- Facilitate integration with a PostgreSQL instance for metadata cataloging or source simulation.
- Support development workflows without requiring local installation of Spark or PostgreSQL.

**Expected container services:**
- **Spark** (driver and executor configuration suitable for local mode)
- **PostgreSQL** (initialized with schema and optional sample data)
- **Iceberg dependencies** (included via Spark session or external jars mounted in the container)

## 1. Prerequisites Checklist

Before starting the Docker environment, ensure the following prerequisites are met:

- **Docker Installed:**  
  Verify Docker is installed and the daemon is running:
    ```
      docker --version
      docker info
    ```
- **Docker Compose Installed:**  
  Confirm that Docker Compose is available:
    ```
      docker-compose --version
    ```
    **NOTE:** In newer versions of docker the `docker-compose` command may be `docker compose`, if using Docker Desktop there should be an option to allow both commands to work.

  **NOTE:** Warnings like `The requested image’s platform (linux/amd64) does not match the detected host platform (linux/arm64/v8)` may happen if your machine has an ARM chip, this isn't neccessarily a problem.
  
- **System Permissions:**  
  Docker must be able to access your file system and network. On macOS/Windows, ensure your shared directories are configured correctly in Docker Desktop settings.

- **Network and Disk Space Requirements:**  
  Ensure you have a stable internet connection for pulling images and at least 4 GB of free disk space.

- **Recommended System Resources:**  
  Allocate at least 2 CPUs and 4 GB of memory to Docker via Docker Desktop settings. Insufficient resources may cause services to fail silently or perform poorly.

## 2. Common Startup Issues

This section outlines frequent problems encountered during the initial launch of the Docker environment and provides solutions to resolve them.

### 2.1 Docker Daemon Not Running

**Symptoms:**
- `docker-compose up` returns a connection error.
- `docker ps` results in an error message like `Cannot connect to the Docker daemon`.

**Resolution Steps:**
- Ensure Docker Desktop or the Docker engine is running.
- Restart Docker from your system tray or command line:
    ```
      sudo systemctl start docker   # Linux
      open -a Docker                # macOS
    ```
- Retry your command once Docker is fully initialized.

### 2.2 Port Conflicts

**Symptoms:**
- Startup fails with a message like `Bind for 0.0.0.0:5432 failed: port is already allocated`.
- PostgreSQL or Spark UI does not load in a browser.
(**Note:** For the docker-compose file in this repo, port 5432 was mapped to 5435 on the host to avoid conflicts with existing local postgres instances)

**Resolution Steps:**
- Check for conflicting services on required ports (e.g., 5432 for PostgreSQL).
- Use `lsof` or `netstat` to identify existing processes:
    ```
      lsof -i :5432
      sudo netstat -tuln | grep 5432
    ```
- If necessary, edit `docker-compose.yml` to change the exposed port:
    ```
      ports:
        - "5433:5432"
    ```
- Restart the environment:
    ```
      docker-compose down
      docker-compose up --build
    ```
### 2.3 Pull Errors or Network Timeouts

**Symptoms:**
- Docker cannot download images.
- Errors like `network timed out` or `TLS handshake timeout`.

**Resolution Steps:**
- Verify internet connectivity and proxy settings.
- Retry the pull with verbose logging:
    ```
      docker-compose pull --quiet
    ```
- If using a corporate network, check Docker’s proxy configuration.
- Consider using a mirror registry or pulling images manually if the issue persists:
    ```
      docker pull postgres:latest
      docker pull bitnami/spark:latest
    ```
## 3. Service-Specific Troubleshooting

This section provides targeted solutions for common problems associated with individual services defined in the Chapter 2 Docker environment.

### 3.1 PostgreSQL Container Issues

**Connection Refused:**
- Ensure the container is running:
    ```
      docker ps
    ```
- Check logs for startup errors:
    ```
      docker logs <postgres_container_name>
    ```
- Confirm the correct port is exposed (e.g., 5432 or remapped alternative).

**Incorrect Credentials or Missing Init Scripts:**
- Verify that the environment variables (e.g., `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`) match your connection configuration.
- If using an initialization SQL script, ensure it is mounted correctly and has no syntax errors:
    ```
      volumes:
        - ./ch2/postgres.sql:/docker-entrypoint-initdb.d/init.sql
    ```
### 3.2 Spark Container Issues

**No Output or Logs Missing:**
- Validate container health and logs:
    ```
      docker logs <spark_container_name>
    ```
- Make sure you're running the PySpark job from inside the Spark container or using proper `spark-submit` commands.

**Job Submission Failures:**
- Check the PySpark script for syntax errors or missing dependencies.
- Review the `spark-defaults.conf` or environment variables for issues with Iceberg or Hadoop integration.

**PySpark Compatibility Issues:**
- Ensure your script uses the right Python version supported by the image.
- Match the PySpark version with the Iceberg version specified in your Dockerfile or mounted JARs.

### 3.3 Metadata or Volume Issues

**Volume Mounting Errors:**
- On Windows or macOS, verify that Docker Desktop has access to the correct file paths.
- Check `volumes` in `docker-compose.yml` for typos or incorrect relative paths.

**File Permission Conflicts:**
- If shared volumes are not writable, adjust permissions on the host:
    ```
      chmod -R 775 ./ch2
    ```
- Alternatively, run containers with a less restrictive user setting or use `chown` in an entrypoint script.

**Persistent Metadata Not Updating:**
- If metadata is not reflecting changes, ensure the metadata directory is mounted properly and not being reset:
    ```
      volumes:
        - iceberg_metadata:/opt/iceberg/metadata
    ```

## 4. Validating Environment

Once your containers are up and running, it's important to validate that each service is functioning as expected before proceeding with the hands-on exercises.

### Verifying Container Health

- Confirm that all expected containers are listed and running:
    ```
      docker ps
    ```
- Use logs to check for healthy startup:
    ```
      docker logs <container_name>
    ```
  Look for messages such as:
  - PostgreSQL: `database system is ready to accept connections`
  - Spark: `Starting Spark master at` or `Spark context available`

### Testing PostgreSQL Connectivity

You can test the connection using a PostgreSQL client (e.g., `psql`) from your host machine or directly from within another container:

- From host (adjust port if remapped):
    ```
      psql -h localhost -p 5432 -U postgres
    ```
- From within a container:
    ```
      docker exec -it <spark_container_name> psql -h postgres -U postgres
    ```
Once connected, verify the sample schema or data if an initialization script was provided:
    ```
      \dt       # List tables
      SELECT * FROM your_table LIMIT 5;
    ```
### Running the Sample PySpark Script

Run the script using `spark-submit` from within the container:
    ```
      docker exec -it <spark_container_name> /opt/spark/bin/spark-submit /mnt/ch2/example.py
    ```
Check the output for confirmation of successful Iceberg table creation, data insertion, and queries.

### Confirming Iceberg Setup

- Review the output to verify that Iceberg tables are being created and manipulated.
- If using a local or embedded catalog, confirm that metadata files are written to the expected volume or directory.
- For Hive or Glue catalogs, check that tables appear via metastore CLI or web UI.

### Validating UI Access (Optional)

If your setup exposes ports for Spark UI:

- Visit `http://localhost:4040` (or the remapped port) to inspect Spark jobs and stages.
- Use this to diagnose task execution, job failure points, or bottlenecks.

## 5. Cleaning Up and Resetting

If the environment becomes unstable, unresponsive, or misconfigured, a clean reset can help resolve persistent issues. This section outlines how to safely stop services, remove resources, and rebuild from a clean state.

### Stopping All Containers

To gracefully stop all running containers defined in your `docker-compose.yml` file:
    ```
    docker-compose down
    ```
If you need to stop all running containers on your system (use with caution):
    ```
    docker stop $(docker ps -q)
    ```
### Removing Volumes and Networks

Volumes and networks created by Docker Compose can persist data and settings. To remove them:

- With volumes and networks:
    ```
      docker-compose down -v --remove-orphans
    ```
- List volumes and delete selectively:
    ```
      docker volume ls
      docker volume rm <volume_name>
    ```
- Remove unused networks:
    ```
      docker network prune
    ```
### Pruning Docker Resources

If disk space or image clutter is an issue, you can clean up unused Docker objects:
    ```
    docker system prune -a
    ```
**Warning:** This removes all stopped containers, unused networks, and dangling images. Make sure this won’t affect other projects.

### Rebuilding the Environment

After cleanup, rebuild and restart the containers:
    ```
    docker-compose up --build
    ```
This ensures fresh images are used, and any misconfigurations or outdated layers are discarded.

### Resetting PostgreSQL or Metadata

If schema or metadata inconsistencies are causing errors:

- Remove the volume storing database or Iceberg metadata.
- Rebuild the environment with the proper initialization scripts:
    ```
      docker-compose down -v
      docker-compose up --build
    ```
Ensure your SQL or PySpark initialization scripts are correct and in place before restarting.

## 6. Diagnostic Tools and Logs

When containers fail to start, hang, or produce unexpected behavior, logs and interactive debugging can help isolate the problem. This section covers practical Docker commands for inspecting and diagnosing issues.

### Viewing Container Logs

To view real-time logs for a specific container:
    ```
    docker logs -f <container_name>
    ```
Use this to monitor startup processes and catch error messages. For example:
    ```
    docker logs -f iceberg_spark_1
    ```
You can also output a fixed number of recent log lines:
    ```
    docker logs --tail 50 <container_name>
    ```
### Accessing Containers Interactively

To run an interactive shell inside a running container:
    ```
    docker exec -it <container_name> /bin/bash
    ```
This is especially useful for:

- Exploring mounted directories
- Inspecting environment variables
- Manually running scripts like `spark-submit` or `psql`

Example:
    ```
    docker exec -it iceberg_spark_1 /bin/bash
    ```
### Inspecting Container Metadata

To view details such as environment variables, mounted volumes, and network settings:
    ```
    docker inspect <container_name>
    ```
Filter specific sections using `jq` or by grepping the output:
    ```
    docker inspect <container_name> | grep IPAddress
    ```
### Using `docker-compose` Diagnostics

- View combined logs from all services:
    ```
      docker-compose logs
    ```
- Restart specific services:
    ```
      docker-compose restart spark
    ```
- Recreate containers from scratch:
    ```
      docker-compose up --force-recreate
    ```
### Debugging Resource Constraints

If containers crash unexpectedly, you may be exceeding resource limits. Monitor usage:

- Docker Desktop resource settings (CPU, RAM)
- Use system tools like `top`, `htop`, or Docker stats:
    ```
      docker stats
    ```
Increase resource allocation in Docker Desktop settings if necessary, then restart the environment.

## 7. FAQs

This section addresses frequently encountered questions and common scenarios when working with the Docker setup in Chapter 2.

### Why is my data not persisting across container restarts?

Data persistence depends on proper volume configuration. If volumes are not declared or mapped correctly, container filesystems are ephemeral.

**Solution:**
- Ensure volumes are declared in `docker-compose.yml`.
- For local data persistence, mount host directories explicitly:
    ```
      volumes:
        - ./data:/var/lib/postgresql/data
    ```
- Avoid using `docker-compose down -v` unless you intend to clear all data.

### How do I expose services to the host?

Docker containers can expose internal ports to your host system using the `ports` field in `docker-compose.yml`.

**Example:**
    ```
      ports:
        - "5432:5432"
    ```
Access PostgreSQL from your host via `localhost:5432`. If the port is in use, remap to a free one:
    ```
      ports:
        - "5433:5432"
    ```
Then connect using:
    ```
    psql -h localhost -p 5433 -U postgres
    ```
### What do I do if a container crashes immediately after startup?

**Check Logs:**
    ```
    docker logs <container_name>
    ```
**Common causes:**
- Misconfigured environment variables
- Missing mount paths
- Port conflicts or permission errors

**Fix:**
- Correct the `docker-compose.yml` or mounted scripts.
- Rebuild the container:
    ```
      docker-compose up --build
    ```
### Can I modify the Docker environment for a custom Spark or PostgreSQL version?

Yes. Edit the `docker-compose.yml` and Dockerfiles to specify alternative image versions or custom builds.

**Example:**
    ```
      image: bitnami/spark:3.4
    ```
Ensure compatibility between Spark, Iceberg, and Python versions to avoid runtime issues.

### How can I verify that Apache Iceberg is functioning correctly?

- Confirm your PySpark job runs successfully with Iceberg catalog configuration.
- Validate metadata files are being written to the expected location.
- Use time travel or schema evolution features in test queries to confirm full functionality.

## 8. Additional Resources

For further learning, diagnostics, or configuration help, the following resources provide official documentation, community support, and practical guides.

### Official Documentation

- **Docker Documentation**  
  https://docs.docker.com/  
  Comprehensive resource on Docker CLI, Compose, volumes, networking, and configuration.

- **Apache Spark Documentation**  
  https://spark.apache.org/docs/latest/  
  Covers Spark’s deployment modes, streaming support, and integration options.

- **Apache Iceberg Documentation**  
  https://iceberg.apache.org/docs/  
  Includes API references, catalog configuration, and compatibility notes.

- **PostgreSQL Documentation**  
  https://www.postgresql.org/docs/  
  Reference for SQL syntax, configuration, and administrative tools.

### Tools and Utilities

- **Docker Desktop** (macOS/Windows)  
  A GUI for managing containers, resource allocation, and logs:  
  https://www.docker.com/products/docker-desktop/

- **pgAdmin**  
  A web-based UI for PostgreSQL that simplifies inspection and querying.

- **Spark UI**  
  Accessible via exposed port (default: `http://localhost:4040`) for live monitoring of Spark job execution.

### Community and Support

- **Apache Iceberg Slack**  
  https://join.slack.com/t/apache-iceberg/shared_invite/  
  Engage with the Iceberg community for advice and updates.

- **Stack Overflow**  
  Use tags like `docker`, `apache-spark`, `postgresql`, and `apache-iceberg` to search or post questions.

- **GitHub Issues**  
  For bugs or requests related to the companion repo, open an issue on the repository.

---

By using this guide, you should be able to resolve most of the setup and runtime issues related to the Docker-based environment used in Chapter 2. For environment-specific adjustments, always validate configuration consistency and container compatibility.
