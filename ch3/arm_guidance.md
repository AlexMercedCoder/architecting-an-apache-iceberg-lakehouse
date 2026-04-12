# Running on ARM Devices (Apple Silicon M1/M2/M3)

The Docker image used for Spark in this chapter (`alexmerced/spark35nb`) was built for the **x86_64 (AMD64)** architecture.

If you are using a device with an ARM chip (such as a MacBook with an M1, M2, or M3 chip), you may encounter issues or warning messages about platform mismatch.

## Solution

Docker Desktop generally handles cross-architecture emulation automatically (especially if "Use Rosetta for x86/amd64 emulation" is enabled in your Docker settings). However, if you run into issues, you can explicitly tell Docker to run the container using x86 emulation.

### Updating Docker Compose

In your `docker-compose.yml` file, add the `platform` key to the `spark` service:

```yaml
  spark:
    image: alexmerced/spark35nb:latest
    container_name: spark
    platform: linux/amd64  # <--- Add this line
    networks:
      lakehouse-net:
    # ... rest of configuration
```

### Performance Note

Please be aware that running x86 containers on ARM architecture via emulation can be slower than running native images. For the purposes of the examples in this book, the performance difference should be negligible, but it is something to keep in mind.
