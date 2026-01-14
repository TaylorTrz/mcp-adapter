# MCP Adapter

## Introduction

**MCP Adapter** is a tool designed to automatically convert OpenAPI specifications (v2/v3) into MCP (Model Context Protocol) applications. It enables seamless transformation of HTTP APIs into MCP APIs, allowing legacy or new HTTP services to be exposed and managed via the MCP protocol with minimal manual intervention.

## Thoughts and Architecture

The project is built around an **event-driven, decoupled architecture**. The core workflow is:

1. **Resource Watcher**: Monitors file changes or Kubernetes ConfigMaps for OpenAPI specs.
2. **OpenAPI Loader**: Parses and validates OpenAPI documents, extracts API routes, and prepares them for MCP conversion.
3. **MCP Server**: Dynamically creates and manages MCP servers and tools based on the parsed OpenAPI specs.
4. **HTTP Server**: Provides health checks and introspection endpoints.

Each stage communicates via asynchronous channels (queues), ensuring loose coupling and scalability. The system supports both file-based and Kubernetes-based resource watching, making it flexible for different deployment scenarios.

**High-level flow:**

```
Resource Watcher (File or Kubernetes)
    │
    └─(channel 1: watcher → openapi)─▶ OpenAPILoader (parses OpenAPI spec, builds HTTP client definitions)
            │
            └─(channel 2: openapi → server)─▶ MCPServer (manages MCP instances and tools)
                    │
                    └─▶ MCPInstance / FastMCP (runs the actual MCP protocol server)
```

**Key Design Points:**

- **Clear module boundaries**: Each responsibility (watching, parsing, serving) is isolated for maintainability.
- **Event-driven with channels**: Asynchronous message passing decouples components.
- **Extensible**: Easily add new resource sources or protocols.
- **Graceful shutdown**: Listens for SIGINT/SIGTERM and cleans up all tasks.


## Dependencies

- Python >= 3.14 (recommend adjusting to 3.11/3.12 for broader compatibility)
- [anyio](https://github.com/agronholm/anyio) — async concurrency
- [argparse](https://docs.python.org/3/library/argparse.html) — CLI parsing
- [fastmcp](https://github.com/modelcontextprotocol/fastmcp) — MCP server framework
- [httpx](https://www.python-httpx.org/) — async HTTP client
- [kopf](https://kopf.readthedocs.io/) — Kubernetes operator framework
- [openapi-spec-validator](https://github.com/p1c2u/openapi-spec-validator) — OpenAPI validation
- [prance](https://github.com/jfinkhaeuser/prance) — OpenAPI parsing and conversion


## TODO

- **Refactor CLI parsing**: Move CLI argument parsing out of module top-level to avoid side effects on import.
- **Unify async runtime**: Standardize on either anyio or asyncio for all concurrency primitives.
- **Graceful server lifecycle**: Ensure HTTP and MCP servers can be started and stopped cleanly.
- **Define strict message types**: Use dataclasses for channel messages to improve type safety.
- **Complete OpenAPI loader logic**: Finalize route extraction, diffing, and error handling.
- **Enhance error handling and retries**: Add robust exception management and retry strategies.
- **Lower Python version requirement**: Update `pyproject.toml` for compatibility with mainstream Python versions.
- **Add unit tests and CI**: Cover core logic with automated tests and continuous integration.
- **Improve configuration management**: Consider using Pydantic's `BaseSettings` for unified config via env/CLI/file.
- **Add structured logging and metrics**: Support JSON logs and Prometheus metrics for observability.

---

For more details, see the [source code](src/mcp_adapter/) and [sample OpenAPI specs](src/samples/).
