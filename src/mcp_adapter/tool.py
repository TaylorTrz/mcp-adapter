import argparse
import asyncio
import hashlib
import logging
import os
import pathlib
import uuid

from dataclasses import dataclass, field
from datetime import datetime, UTC
from rich.console import Console
from rich.logging import RichHandler
from typing import Any, List, Dict, Tuple, Optional
from pydantic import BaseModel

APP = "mcp-adapter"
SUPPORT_SPEC_FILE_SUFFIX = [".json"]

ACTION_ADD = "ADDED"
ACTION_DELETE = "DELETED"
ACTION_MODIFY = "MODIFIED"

LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(asctime)s %(levelname)s [Uvicorn] %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(asctime)s %(levelname)s [Uvicorn] %(client_addr)s - "%(request_line)s" %(status_code)s',  # noqa: E501
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",  # logging.StreamHandler
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",  # logging.StreamHandler
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}

log = logging.getLogger(APP)
    
class Resource(BaseModel):
    uid: str
    name: str
    data: dict
    label: str
    action: str
    
    
class OpenAPI(BaseModel):
    """An OpenAPI message model"""
    
    uid: str
    
    """Mapping to resource.name"""
    name: str
    
    """Mapping to a HTTP client base_url"""
    base_url: str
    
    """Mapping to a MCP server listen port."""
    port: str

    """Action like ["ADDED", "DELETED"]"""
    action: str
    
    """OpenAPI spec to add."""
    spec: Optional[Dict[str, Any]]

    """HTTP route to add, mapping to a MCP server's tool.name."""
    route_names: Dict[str, str]
    
    """HTTP route to delete, mapping to a MCP server's tool.name."""
    stale_routes: List[str]


class Message(BaseModel):
    uid: str
    version_id: str
    payload: Resource|OpenAPI


@dataclass
class Channel():
    name: str
    queue: asyncio.Queue[Message]

    async def send(self, uid: str, payload: Resource|OpenAPI) -> None:
        def _version_uid() -> str:
            return "%s-%s" % (
                datetime.now(UTC).strftime(f'%Y%m%d%H%M%S'), uuid.uuid4().hex[:8]
                )
        message = Message(uid=uid,
                          payload=payload,
                          version_id=_version_uid())
        log.debug(f"Channel [{self.name}] send payload uid: {uid}|version_id: {message.version_id}")
        await self.queue.put(message)
        

    async def recv(self, timeout: float = -1.0) -> Tuple[str, Resource|OpenAPI]:
        if timeout < 0.0:
            message = await self.queue.get()
        else:
            message = await asyncio.wait_for(self.queue.get(), timeout=timeout)

        log.debug(f"Channel [{self.name}] receive payload uid: {message.uid}|version_id: {message.version_id}")
        return message.uid, message.payload


class FileTool:

    @classmethod
    def mtime(cls, file: str, prev: str | None) -> str:
        """Check if a file was modified"""
        stat = pathlib.Path(file).stat()
        mtime = str(datetime.fromtimestamp(stat.st_mtime))
        return mtime if prev != mtime else str()

    @classmethod
    def validate(cls, file: str) -> bool:
        "Validate if a file exists and accessable"
        path = pathlib.Path(file)
        return path.is_file() and path.exists() and path.suffix in SUPPORT_SPEC_FILE_SUFFIX

    @classmethod
    def read(cls, file: str, max_char: int = -1) -> str:
        "Read a text file with max_char limitation"
        with pathlib.Path(file).open("r+", encoding="utf-8") as fd:
            return fd.read(max_char)


class HashTool:
    @classmethod
    def md5sum(cls, content: str | None) -> str:
        checksum = bytes()
        if content:
            hash_obj = hashlib.md5(content.encode('utf-8'))
            checksum = hash_obj.digest()
        return str(checksum)
    
    @classmethod
    def _fnv1a_64(cls, data: bytes) -> int:
        h = 0xcbf29ce484222325
        for b in data:
            h ^= b
            h *= 0x100000001b3
            h &= 0xffffffffffffffff
        return h

    @classmethod
    def _base62_encode(cls, num: int) -> str:
        alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        s = []
        while num > 0:
            num, r = divmod(num, 62)
            s.append(alphabet[r])
        return "".join(reversed(s)) or "0"

    @classmethod
    def short64(cls, text: str):
        return cls._base62_encode(cls._fnv1a_64(text.encode()))



class ConfigTool:

    def __init__(self):
        self.conf: argparse.Namespace
        self.loggers: Dict[str, logging.Logger] = {}
        self.initialize()
        
    def initialize(self):
        self.setup_options()
        self.setup_logging()
        
    def setup_options(self):
        parser = argparse.ArgumentParser(description="MCP Adapter v1.0.0")

        # Default group
        default_group = parser.add_argument_group("default", "Default options")
        default_group.add_argument("--name", type=str, help="Adapter http server name", required=False,
                                   default=os.getenv("APP_NAME", "mcp-adapter"))
        default_group.add_argument("--host", type=str, help="Adapter http server listen host", required=False,
                                   default=os.getenv("APP_HOST", "0.0.0.0"))
        default_group.add_argument("-p", "--port", type=int, help="Adapter http server listen port", required=False,
                                   default=int(os.getenv("APP_PORT", 8080)))
        default_group.add_argument("-l", "--log-level", type=str, help="Log level", required=False,
                                   default=os.getenv("LOG_LEVEL", "DEBUG"))

        # File resource group
        file_group = parser.add_argument_group("file resource", "File options")
        file_group.add_argument("-f", "--file", action="append", type=str, help="File to watch", default=[])
        file_group.add_argument("-d", "--dir", action="append", type=str, help="Directory to watch", default=[])
        file_group.add_argument("--port-min", type=str, help="MCP listen port range minimun", default=30000, required=False)
        file_group.add_argument("--port-max", type=str, help="MCP listen port range maximun", default=31000, required=False)

        # Kubernetes resource group
        kopf_group = parser.add_argument_group("kopf resource", "Kubernetes options")
        kopf_group.add_argument("-k", "--kopf", action="store_true", help="Require kopf watcher", required=False)
        kopf_group.add_argument("--namespace", type=str, help="Kubernetes namespace", required=False,
                                    choices=["apig"], default=os.getenv("NAMESPACE", "apig"))
        kopf_group.add_argument("--user-label", type=str, help="Kubernetes configmap label root_user_id", required=False,
                                    default=os.getenv("ROOT_USER_ID", ""))
        
        # OpenAPI group
        openapi_group = parser.add_argument_group("openapi", "OpenAPI options")
        openapi_group.add_argument("--base-url", type=str, help="HTTP client base url", required=False,
                                   default=os.getenv("BASE_URL", "http://localhost"))

        arguments: argparse.Namespace = parser.parse_args()
        self.conf = arguments
        
    def setup_logging(self):
        log_format = "%(asctime)s %(levelname)s [Adapter]" \
            " |%(processName)s|%(threadName)s|%(taskName)s|" \
            " %(module)s.%(funcName)s.%(lineno)s: %(message)s"
        
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(self.conf.log_level)
        consoleHandler.setFormatter(logging.Formatter(log_format))
        
        richHandler = RichHandler(console=Console(stderr=True),
                                  log_time_format=f"%Y-%m-%d %H:%M:%S",
                                  locals_max_string=80)
        richHandler.setLevel(self.conf.log_level)
        # richHandler.setFormatter(logging.Formatter(log_format))

        logging.basicConfig(level=self.conf.log_level, handlers=[consoleHandler])
        
    def get_logger(self, name):
        if name in self.loggers:
            return self.loggers[name]
        return logging.getLogger(__name__)

    def __repr__(self):
        return str({"arguments": str(self.conf), "loggers": str(self.loggers)})

config = ConfigTool()
