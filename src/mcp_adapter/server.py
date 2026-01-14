import asyncio
import httpx
import logging

from fastmcp import FastMCP
from typing import Optional, Any, Generic, Literal, List, cast, Dict
from fastmcp.experimental.server.openapi import FastMCPOpenAPI
from fastmcp.experimental.server.openapi.components import OpenAPITool
from fastmcp.experimental.server.openapi.routing import ComponentFn, RouteMap, RouteMapFn
from fastmcp.experimental.utilities.openapi import HTTPRoute

from mcp_adapter import tool


log = logging.getLogger(tool.APP)

CONF = tool.config.conf
Transport = Literal["stdio", "http", "sse", "streamable-http"]


class MCPServer(FastMCPOpenAPI):
    """
    FastMCP server implementation that creates components from an OpenAPI schema.

    This class parses an OpenAPI specification and creates appropriate FastMCP components
    (Tools, Resources, ResourceTemplates) based on route mappings.

    Example:
        ```python
        from fastmcp.server.openapi import FastMCPOpenAPI, RouteMap, MCPType
        import httpx

        # Create server with custom mappings and route mapper
        server = FastMCPOpenAPI(
            openapi_spec=spec,
            client=httpx.AsyncClient(),
            name="API Server",
            route_maps=custom_mappings,
        )
        ```
    """

    def __init__(
        self,
        openapi_spec: dict[str, Any],
        client: httpx.AsyncClient,
        name: str | None = None,
        route_maps: list[RouteMap] | None = None,
        route_map_fn: RouteMapFn | None = None,
        mcp_component_fn: ComponentFn | None = None,
        mcp_names: dict[str, str] = {},
        tags: set[str] | None = None,
        timeout: float | None = None,
        **settings: Any,
    ):
        self._mcp_names = mcp_names

        # NOTE: (taylor) MCP component field 'title'
        #       https://github.com/jlowin/fastmcp/pull/982
        #       https://github.com/modelcontextprotocol/modelcontextprotocol/pull/663
        #       A title field provides a human-readable display name,
        #       while keeping the existing name field for programmatic identification.
        def _mcp_component_fn(route, tool):
            if tool.name in mcp_names:
                tool.title = mcp_names[tool.name]
                log.debug(
                    f"-> A mcp tool {tool.name} update title to {tool.title}")
        mcp_component_fn = _mcp_component_fn

        super().__init__(openapi_spec, client, name, route_maps, route_map_fn,
                         mcp_component_fn, mcp_names, tags, timeout, **settings)
        log.info(
            f"Created openapi mcp server with {len(mcp_names)} routes: {mcp_names}")

    def _generate_default_name(
        self, route: HTTPRoute, mcp_names_map: dict[str, str] | None = None
    ) -> str:
        """Generate a default name from the route using the configured strategy."""
        mcp_names_map = mcp_names_map or {}

        # First check if there's a custom mapping for this operationId
        if not (route.operation_id and route.operation_id in mcp_names_map):
            log.info(
                f"-> A route {route.operation_id} not mapped in {mcp_names_map}.")
            return "Default"

        name = mcp_names_map[route.operation_id]

        # Truncate to 56 characters maximum, [prefix:43, :2, operation_id:11]
        if len(name) > 43:
            name = name[:43]
        return name + "__" + route.operation_id

    def _create_openapi_tool(
        self,
        route: HTTPRoute,
        name: str,
        tags: set[str],
    ):
        """Creates and registers an OpenAPITool with enhanced description."""
        if name.split("__")[-1] not in self._mcp_names:
            log.debug(
                f"A route {name} was not explicitly added into mcp_names {self._mcp_names}")
            return

        super()._create_openapi_tool(route, name, tags)


class MCPManager():

    def __init__(self) -> None:
        self.alias = "[mcp_server]"
        self.openapi_chan: tool.Channel
        self.cached_tasks: Dict[str, asyncio.Task] = {}
        self.cached_servers: Dict[str, Optional[FastMCP]] = {}

    async def create_server(self, base_url, port, openapi_spec, openapi_routes):
        server_name_schema = f"[OpenAPI-MCP-{base_url}:{port}]"
        server: FastMCP = MCPServer(
            openapi_spec=openapi_spec,
            client=httpx.AsyncClient(base_url=base_url),
            name=server_name_schema,
            mcp_names=openapi_routes)
        return server

    async def run_server(self, server: FastMCP, port: str,
                         host: str = "0.0.0.0",
                         show_banner: bool = False,
                         transport: Transport = "streamable-http"):
        """Run and cache a mcp server with uvicorn.
        
        NOTE: (taylor) A stateless HTTP server
              Use the streamable HTTP transport in stateless mode
        https://github.com/jlowin/fastmcp/issues/1640
        
        TODO: (TaylorT) Uvicorn resource limits and process management
            * --workers INTEGER, Number of worker processes.
            * --limit-concurrency <int>, Maximum number of concurrent connections or
                tasks to allow, before issuing HTTP 503 responses. Useful for ensuring
                known memory usage patterns even under over-resourced loads.
            * --limit-max-requests <int>, Maximum number of requests to service before
                terminating the process. Useful when running together with a process
                manager, for preventing memory leaks from impacting long-running processes.
        https://uvicorn.dev/settings/#resource-limits
        https://uvicorn.dev/deployment/#using-a-process-manager
        """
        asyncio.create_task(server.run_async(
            host=host,
            port=int(port),
            show_banner=show_banner,
            transport=transport,
            stateless_http=True,
            log_level=CONF.log_level.lower(),
            uvicorn_config={'log_config': tool.LOGGING_CONFIG}
        )
        )
        self.cached_servers[port] = server
        return server

    async def close_server(self, uid: str) -> None:
        """todo: (taylor) Close a running mcp server."""
        ...

    async def on_notify(self, action: str,
                        openapi: tool.OpenAPI) -> Optional[FastMCP]:
        """Notify mcp server and add/remove tool on any action.
        
        NOTE: (TaylorT)
            A mcp tool can be dynamicly added or removed, then send notification to
            a mcp client with event tools/list_changed.
        https://fastmcp.wiki/zh/servers/tools
        """
        server_cache = self.cached_servers.get(openapi.port)
        log.debug(f"{self.alias} -> On notification action {action}, an openapi sepc"
                  f" {openapi.model_dump()} will be updated with cache {server_cache}.")

        if action == tool.ACTION_ADD:
            if openapi.spec:
                server = await self.create_server(openapi.base_url,
                                                  openapi.port,
                                                  openapi.spec,
                                                  openapi.route_names)
                if not server_cache:
                    log.info(f"{self.alias} -> An empty cache entry on action {action},"
                             f" Normally occur on first-time listing or newly created resource.")
                    await self.run_server(server, openapi.port)
                else:
                    # MCP server composition:
                    #   https://fastmcp.wiki/zh/servers/composition
                    #   With 'mount' or 'import_server', a mcp server can be connected to pre-exists
                    #   server. A 'mount' action is dynamic while 'import_server' is static action.
                    await server_cache.import_server(server)

        if openapi.stale_routes and action in [tool.ACTION_DELETE, tool.ACTION_MODIFY]:
            if not server_cache:
                log.error(f"{self.alias} -> An empty cache entry on action {action},"
                          f" which must error out on this notification.")
                return
            mcp_tools = await server_cache.get_tools()

            mcp_name_maps: Dict[str, str] = {}
            for mcp_name, _ in mcp_tools.items() if mcp_tools else {}:
                mcp_name_maps[mcp_name.split("__")[-1]] = mcp_name

            for stale_route in openapi.stale_routes:
                tool_name = mcp_name_maps.get(stale_route)
                if not tool_name:
                    log.warning(
                        f"{self.alias} -> A stale route {stale_route} missing in mcp tools")
                    continue
                log.debug(f"-> removing stale route {stale_route} from mcp tool {tool_name}")
                server_cache.remove_tool(tool_name)

        if openapi.spec and action == tool.ACTION_MODIFY:
            if openapi.spec:
                server = await self.create_server(openapi.base_url,
                                                  openapi.port,
                                                  openapi.spec,
                                                  openapi.route_names)
                if not server_cache:
                    log.info(f"{self.alias} -> An empty cache entry on action {action},"
                             f" which is an unexpected notification, reconciliation and create.")
                    await self.run_server(server, openapi.port)
                else:
                    await server_cache.import_server(server)

        return server_cache

    async def run(self, channel: tool.Channel, exit_event: asyncio.Event) -> None:
        self.openapi_chan = channel

        while not exit_event.is_set():
            try:
                (port, openapi) = await self.openapi_chan.recv(timeout=0.1)
                log.debug(f"{self.alias} -> MCP controller received openapi"
                          f" [port: {port}|uid: {openapi.uid}|name: {openapi.name}]")

                if (port and openapi):
                    assert isinstance(openapi, tool.OpenAPI)
                    await self.on_notify(openapi.action, openapi)

                log.debug(f"{self.alias} -> MCP controller processed openapi"
                          f" [port: {port}|uid: {openapi.uid}|name: {openapi.name}]")

                await asyncio.wait_for(exit_event.wait(), timeout=0.1)

            except asyncio.TimeoutError:
                continue  # Normal timeout, proceed to next check
            except Exception as e:
                log.error(
                    f"Woops! unexpected error while running a mcp server: {e}", exc_info=True)

        log.warning("MCP controller exiting as exit_event is set.")


mcp_manager = MCPManager()
