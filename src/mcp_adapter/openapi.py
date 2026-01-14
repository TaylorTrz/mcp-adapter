import asyncio
import json
import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from openapi_pydantic import parse_obj
from openapi_pydantic.v3 import parser as openapi_pydantic_parser
from openapi_pydantic.v3 import v3_0 as openapi_pydantic_v30
from openapi_pydantic.v3 import v3_1 as openapi_pydantic_v31
from typing import Dict, List, Any, Optional, NamedTuple, Union
from prance import ResolvingParser
from prance import convert as convert_v2
from prance.util import resolver
from pydantic_core import _pydantic_core as pydantic_exp

from mcp_adapter import tool
from mcp_adapter.tool import HashTool

log = logging.getLogger(tool.APP)
CONF = tool.config.conf

OPENAPI_V2 = "2.0"  # Prance support OpenAPI v2.0/v3.0
OPENAPI_V30 = "3.0.0"  # FastMCP/openapi-pydantic support v3.0/v3.1
OPENAPI_V31 = "3.1.0"
HTTP_METHODS = ["get", "post", "put", "patch", "delete", "options", "head"]

OpenAPIModel = openapi_pydantic_parser.OpenAPIv3
OperationModel = Union[openapi_pydantic_v30.Operation,
                       openapi_pydantic_v31.Operation]


@dataclass
class OpenAPIRoute:

    """A route name, mapping to mcp tool's name."""
    name: str
    
    """A route title, mapping to mcp tool's title."""
    title: str
    
    """A route content's md5 checksum."""
    checksum: str


@dataclass
class OpenAPISpec:

    """An openapi spec model."""
    spec: Optional[OpenAPIModel]

    """An openapi route mapping, route.name as the key"""
    routes: Dict[str, OpenAPIRoute]


class OpenAPIChain(ABC):
    """An OpenAPI spec tool, to function as a parser, a validator, and a converter."""

    def __init__(self):
        self.support_version: List[str]
        self.next: Optional[OpenAPIChain]
        super().__init__()

    @abstractmethod
    def detect(self, spec: Any | Dict[str, Any]) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def validate(self, spec: Any | Dict[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def convert(self, spec: Any | Dict[str, Any], target_version: str) -> Any | Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def parse(self, spec: Any | Dict[str, Any]) -> Any | Dict[str, Any]:
        raise NotImplementedError

    def validate_spec(self, spec: Dict[str, Any]) -> bool | None:
        """Validate an OpenAPI spec."""
        if self.detect(spec) in self.support_version:
            return self.validate(spec)
        if self.next:
            return self.next.validate_spec(spec)

    def convert_spec(self, spec: Dict[str, Any], target_version: str = OPENAPI_V30) -> Any | Dict[str, Any]:
        """Convert an OpenAPI spec to a target version."""
        if self.detect(spec) in self.support_version:
            return self.convert(spec, target_version=target_version)
        if self.next:
            return self.next.validate_spec(spec)

    def parse_spec(self, spec: Dict[str, Any],) -> Any | Dict[str, Any]:
        if self.detect(spec) in self.support_version:
            return self.parse(spec)
        if self.next:
            return self.next.parse_spec(spec)


class OpenAPIV2(OpenAPIChain):

    def __init__(self, next_tool: Optional[OpenAPIChain] = None):
        self.alias: str = "[openapi_tool_v2]"
        self.swagger = "swagger"
        self.support_version: List[str] = [OPENAPI_V2]
        self.next_tool: Optional[OpenAPIChain] = next_tool
        super().__init__()

    def detect(self, spec: Any | Dict[str, Any]) -> str | None:
        swagger_version = spec.get(self.swagger, "")
        if swagger_version and isinstance(swagger_version, str):
            # Only Match major version 2.x, ignoring minor.patch version
            if str(swagger_version).startswith("2."):
                return OPENAPI_V2
        log.warning(
            f"{self.alias} version detect fail on spec version {swagger_version}")

    def validate(self, spec: Any | Dict[str, Any]) -> bool:
        return True if self.parse(spec) else False

    def convert(self, spec: Any | Dict[str, Any], target_version: str) -> Any | Dict[str, Any]:
        """

        Prance convert OpenAPI v2.0 to v3.0:
        https://prance.readthedocs.io/en/latest/api/prance.convert.html
        """
        if target_version in [OPENAPI_V30, OPENAPI_V31]:
            return convert_v2.convert_spec(spec)
        return spec

    def parse(self, spec: Any | Dict[str, Any]) -> Dict[str, Any] | None:
        parser = ResolvingParser(
            spec_string=json.dumps(spec),
            lazy=True,
            backend='openapi-spec-validator',  # support both v2.0 and v3.0
            resolve_types=resolver.RESOLVE_INTERNAL
        )
        parser.parse()
        return parser.specification


class OpenAPIV3(OpenAPIChain):

    def __init__(self, next_tool: Optional[OpenAPIChain] = None):
        self.alias: str = "[openapi_tool_v3]"
        self.openapi: str = "openapi"
        self.support_version: List[str] = [OPENAPI_V30, OPENAPI_V31]
        self.next_tool: Optional[OpenAPIChain] = next_tool
        super().__init__()

    def detect(self, spec: Any | Dict[str, Any]) -> str | None:
        openapi_version = spec.get(self.openapi, "")
        if openapi_version and isinstance(openapi_version, str):
            # Match major.minor version 3.x, ignoring patch version
            if str(openapi_version).startswith("3.0"):
                return OPENAPI_V30
            elif str(openapi_version).startswith("3.1"):
                return OPENAPI_V31
        log.warning(
            f"{self.alias} version detect fail on spec version {openapi_version}")

    def validate(self, spec: Any | Dict[str, Any]) -> bool:
        return True if self.parse(spec) else False

    def convert(self, spec: Any | Dict[str, Any], target_version: str) -> Dict[str, Any] | None:
        return spec

    def parse(self, spec: Any | Dict[str, Any]) -> Dict[str, Any] | None:
        try:
            openapi_spec: OpenAPIModel = parse_obj(spec)
            return openapi_spec.model_dump()
        except pydantic_exp.ValidationError as err:
            log.error(f"{self.alias} parser fail: {err}")
        except Exception as e:
            log.error(f"{self.alias} parser with unexpected error: {e}")


class OpenAPILoader:
    """An OpenAPI spec loader."""

    def __init__(self) -> None:
        self.alias: str = "[openapi_loader]"
        # NOTE: (taylor) A cache to store openapi specs
        #       Here we consider resource.name to be cache key rather than resource.label,
        #       as user can split one openapi spec into multipart, and spread to multi Configmaps.
        self.cached_api_routes: Dict[str, Dict[str, List[OpenAPIRoute]]] = {}
        super().__init__()

    @property
    def tool(self):
        openapi_tool_v2 = OpenAPIV2()
        openapi_tool_v3 = OpenAPIV3(next_tool=openapi_tool_v2)
        return openapi_tool_v3

    def parse(self, resource: tool.Resource) -> Dict[str, Optional[OpenAPISpec]]:
        a_pile_of_obj: Dict[str, Optional[OpenAPISpec]] = {}

        obj = self.parse_obj(resource.data)
        if not obj:
            log.warning("%s -> parse failure with resource [uid: %s|name: %s]",
                        self.alias, resource.uid, resource.name)
            return a_pile_of_obj

        root_server_urls = [
            server.url for server in obj.servers] if obj.servers else [CONF.base_url]

        for path, path_item in obj.paths.items() if obj.paths else {}:
            # server in root spec should always be protected
            server_urls = root_server_urls[:]
            if path_item and path_item.servers:
                server_urls = [server.url for server in path_item.servers]

            for method_lower in HTTP_METHODS:
                operation: OperationModel = getattr(path_item, method_lower)
                if operation:
                    if operation and operation.servers:
                        server_urls = [
                            server.url for server in operation.servers]

                    for base_url in server_urls:
                        open_api_spec = a_pile_of_obj.get(base_url)
                        if not open_api_spec:
                            open_api_spec = OpenAPISpec(spec=None, routes={})

                        # NOTE: a route name must be unique in an openapi spec,
                        #       a configmap and a mcp server.
                        route_name = "[%s|%s|%s|%s]" % \
                            (resource.label, base_url, path, method_lower)
                        route_name = HashTool.short64(route_name)
                        route_title = operation.operationId
                        if not route_title:
                            route_title = str(operation.summary)
                        operation.operationId = route_name

                        # An openapi route, check whether content was modified or not
                        operation_json = operation.model_dump_json()
                        open_api_route = OpenAPIRoute(
                            name=route_name, title=route_title,
                            checksum=HashTool.md5sum(operation_json))
                        log.debug(f"{self.alias} -> open api route {route_name}")

                        open_api_spec.routes[route_name] = open_api_route
                        a_pile_of_obj[base_url] = open_api_spec
                        
        # Make each spec with different base_url copy from the same spec obj
        for base_url, open_api_spec in a_pile_of_obj.items():
            if open_api_spec:
                open_api_spec.spec = obj.model_copy()

        if not a_pile_of_obj:
            # NOTE: An openapi resource without any api routes.
            #       This may happen on event 'MODIFIED' to remove all api definitions,
            #       and we should consider carefully on this.
            log.info("%s -> An openapi resource [uid: %s|name: %s] without any api routes",
                     self.alias, resource.uid, resource.name)
            for base_url in root_server_urls:
                a_pile_of_obj[base_url] = OpenAPISpec(
                    spec=obj.model_copy(), routes={})

        log.debug(f"{self.alias} -> parsed a pile of openapi obj from resource:"
                  f" |uid: {resource.uid}|name: {resource.name}")
        return a_pile_of_obj

    def parse_obj(self, spec: Dict[str, Any]) -> OpenAPIModel | None:
        if not self.tool.validate_spec(spec):
            log.warning(f"{self.alias} spec validation fail, ignoring...")
            return

        versioned_spec = self.tool.convert_spec(
            spec, target_version=OPENAPI_V30)

        openapi_spec = self.tool.parse_spec(versioned_spec)
        openapi_obj = parse_obj(openapi_spec)
        return openapi_obj

    async def on_notify(self,
                        action: str,
                        a_pile_of_obj: Dict[str, Optional[OpenAPISpec]],
                        resource: tool.Resource) -> List[tool.OpenAPI]:
        cached_api_route: Dict[str, List[OpenAPIRoute]] = \
            self.cached_api_routes.get(resource.name, {})

        open_api_resources: List[tool.OpenAPI] = []
        
        # Pre-existed route with this base_url
        for base_url, cached_routes in cached_api_route.items():
            # Any route without this base_url should be a staled one
            if base_url not in a_pile_of_obj:
                stale_routes: List[str] = []
                for route in cached_routes if cached_routes else []:
                    stale_routes.append(route.name)
                log.info(f"{self.alias} -> A openapi added with "
                         f"mcp port: {resource.label}, action: {resource.action}, "
                         f"base_url: {base_url}, stale_routes: {stale_routes}")
                open_api = tool.OpenAPI(uid=resource.uid,
                                        name=resource.name,
                                        port=resource.label,
                                        action=resource.action,
                                        base_url=base_url,
                                        spec=None,
                                        route_names={},
                                        stale_routes=stale_routes)
                open_api_resources.append(open_api)
                cached_api_route[base_url] = []

        # Newly added or modified routes with this base_url 
        for base_url, open_api_spec in a_pile_of_obj.items():
            if not (base_url and open_api_spec):
                log.debug(f"{self.alias} -> On notification action {action}, invalid"
                          f" base_url {base_url} or openapi spec {open_api_spec}")
                continue

            spec: Dict[str, Any] = {}
            route_names: Dict[str, str] = {}
            stale_routes: List[str] = []
            if isinstance(open_api_spec.spec, OpenAPIModel):
                spec = open_api_spec.spec.model_dump()

            routes: Dict[str, OpenAPIRoute] = open_api_spec.routes
            cached_routes: List[OpenAPIRoute] = cached_api_route.get(
                base_url, [])
            for cached_route in cached_routes:
                if action != tool.ACTION_DELETE:
                    # Cached route's content unchanged, we still need to update it
                    if cached_route.name in routes \
                            and cached_route.checksum == routes[cached_route.name].checksum:
                        log.debug(f"-> Though route {cached_route.name} "
                                  f"unchanged, we still need to delete it.")
                # Cached route will not alive after current notification
                stale_routes.append(cached_route.name)
                
            for route_name, route in routes.items():
                route_names[route_name] = route.title

            log.info(f"{self.alias} -> A openapi added with "
                        f"mcp port: {resource.label}, action: {resource.action}, "
                        f"base_url: {base_url}, route_names: {route_names}, "
                        f"stale_routes: {stale_routes}")
            open_api = tool.OpenAPI(uid=resource.uid,
                                    name=resource.name,
                                    port=resource.label,
                                    action=resource.action,
                                    base_url=base_url,
                                    spec=spec,
                                    route_names=route_names,
                                    stale_routes=stale_routes)
            open_api_resources.append(open_api)

            # Cache sychronization, take carefully action on first-time adding or last-time deletion
            cached_routes = [route for _, route in routes.items()]
            cached_api_route[base_url] = cached_routes

        if action == tool.ACTION_DELETE:
            cached_api_route = {}

        self.cached_api_routes[resource.name] = cached_api_route
        return open_api_resources

    async def run(self,
                  watcher_chan: tool.Channel,
                  openapi_chan: tool.Channel,
                  exit_event: asyncio.Event) -> None:
        """Asyncronously load spec from watcher channel to openapi channel.

        :param watcher_chan: a channel to receive resource definition
        :param openapi_chan: another channel to send openapi specfication
        :param exit_event: running infinitly until the stop event is set
        """
        while not exit_event.is_set():
            try:
                (name, resource) = await watcher_chan.recv(timeout=0.1)
                log.debug(f"{self.alias} -> OpenAPI loader received resource"
                          f" [uid: {resource.uid}|name: {name}]")

                if name and resource:
                    assert isinstance(resource, tool.Resource)
                    a_pile_of_obj: Dict[str, Optional[OpenAPISpec]] = self.parse(
                        resource)

                    openapi_models: List[tool.OpenAPI] = await self.on_notify(
                        resource.name, a_pile_of_obj, resource)
                    for open_api in openapi_models:
                        # TODO(Taylor): No failure is acceptable considering cache consistency
                        await openapi_chan.send(resource.label, open_api)

                log.debug(f"{self.alias} -> OpenAPI loader processed resource"
                          f" [uid: {resource.uid}|name: {resource.name}]")

                await asyncio.wait_for(exit_event.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                continue  # Normal timeout, proceed to next check
            except Exception as e:
                log.error(
                    f"Woops! unexpected error while processing an openapi spec: {e}", exc_info=True)

        log.warning("OpenAPI loader exiting as exit_event is set.")


openapi_loader = OpenAPILoader()
