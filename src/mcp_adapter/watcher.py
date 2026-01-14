import asyncio
import json
import kopf
import logging
import threading

from abc import ABC, abstractmethod
from kopf._cogs.structs import credentials
from typing import List, Dict, NamedTuple

from mcp_adapter import tool
from mcp_adapter.tool import config, Channel, Resource, FileTool
from mcp_adapter.tool import ACTION_ADD, ACTION_DELETE, ACTION_MODIFY

CONF = config.conf
log = logging.getLogger(tool.APP)

Cache = NamedTuple('Cache', [('mtime', str), ('port', str)])


class ResourceWatcher(ABC):
    def __init__(self) -> None:
        self.chan: Channel
        super().__init__()

    @abstractmethod
    async def run(self, channel: Channel, exit_event: asyncio.Event) -> None:
        raise NotImplementedError

    @abstractmethod
    async def run_in_thread(self, stop_flag: threading.Event) -> None:
        raise NotImplementedError


@kopf.on.login()  # type: ignore
def login_fn(**kwargs):
    """Login to Kubernetes cluster using Kopf.

    kopf/docs/embedding.rst:
    https://kopf.readthedocs.io/en/stable/embedding/
        The login handler is called when Kopf starts, to establish a connection to the Kubernetes cluster.
        By defining a custom login function, you can customize how Kopf authenticates with the cluster.
    """
    connection_info = kopf.login_with_service_account(**kwargs)
    if connection_info is not None:
        connection_info = credentials.ConnectionInfo(
            server=connection_info.server,
            ca_path=connection_info.ca_path,
            # NOTE(Taylor): disable cert verification to ignore wired issue
            insecure=True,
            username=connection_info.username,
            password=connection_info.password,
            scheme=connection_info.scheme,
            token=connection_info.token,
            certificate_path=connection_info.certificate_path,
            private_key_path=connection_info.private_key_path,
            priority=connection_info.priority,
        )
    log.info("Kopf login function called, connection_info: %s",
             str(connection_info))
    return connection_info


@kopf.on.startup()  # type: ignore
def configure(settings: kopf.OperatorSettings, **_):
    """A startup handler to initialize kopf operator.

    kopf.OperatorSetting:
        kopf/_cogs/configs/configuration.py:ScanningSettings
        https://kopf.readthedocs.io/en/latest/configuration/
        Disable dynamic scanning while lacking permissions to list/watch all namespaces.
    """
    settings.scanning.disabled = True


@kopf.on.event('', 'v1', 'configmaps',
               labels={"userid": CONF.user_label})  # type: ignore
async def configmap_handler(**kwargs):
    """A event handler on configmap events.

    docs/kwargs.rst: kwargs is an argument for all handlers
    https://kopf.readthedocs.io/en/latest/kwargs

    For all handlers:
    .resource: kopf._cogs.structs.references.Resource, A actual resource during the initial discovery.
    .body: kopf._cogs.structs.bodies.Body, A body of the resource being handled.
    .spec, .meta, .status: live-view for body['spec'], body['metadata'], body['status']
    .uid, .name, .namespace: aliases for the respective fields in body['metadata'], can be None is not present.
    .labels, .annotations: live-view for body['metadata']['labels'], body['metadata']['annotations']

    Only for event handlers:
    .event: dict, A raw JSON-decoded message, with keys 'type' & 'object'.
    .event.type: str, Event type (e.g., 'ADDED', 'MODIFIED', 'DELETED', ). 'None' only on the first-time listing.
    .event.object: dict, Resource object being handled with event, inner keys ['apiVersion', 'kind', 'metadata', 'data'].
    """
    uid: str = kwargs.get('uid', "")
    name: str = kwargs.get('name', "")
    labels: dict = kwargs.get('labels', {})
    event: dict = kwargs.get('event', {})
    log.info("Kopf event handler receiving configmap [uid: %s, name: %s] -> event: %s",
             uid, name, event['type'])

    if not (labels.get('port') and isinstance(labels['port'], str)):
        log.warning("Configmap [uid: %s | name: %s] label %s invalid",
                    uid, name, labels)
        return

    event_object: dict = event.get('object', {})
    if not (event_object.get('data') and isinstance(event_object['data'], dict)):
        log.warning("Configmap [uid: %s | name: %s] data %s invalid",
                    uid, name, event_object.get('data'))
        return

    object_data: dict = event_object.get('data', {})
    if not (object_data.get('openapi') and isinstance(object_data['openapi'], str)):
        log.warning("Configmap [uid: %s | name: %s] openapi data %s invalid",
                    uid, name, object_data.get('openapi'))
        return

    openapi_data = json.loads(object_data['openapi'])
    resource = Resource(uid=uid,
                        name=name,
                        data=openapi_data,
                        label=labels['port'],
                        action=event['type'] if event.get('type') else ACTION_ADD)

    # Kopf operator runs in another thread, wait until channel is ready
    while not resource_watcher.chan:
        await asyncio.sleep(0.1)
    await resource_watcher.chan.send(resource.name, resource)
    log.info(
        f"A resource [uid: {uid} | name: {name} | action: {resource.action}] successfully sent to channel.")


class KopfResourceWatcher(ResourceWatcher):
    """A Kopf-based Kubernetes Resource Watcher"""

    def __init__(self) -> None:
        self.chan: Channel
        self.stop_flag: threading.Event = threading.Event()
        super().__init__()

    async def run(self, channel: Channel, exit_event: asyncio.Event) -> None:
        """Dummy loop to keep the watcher running."""
        self.chan = channel
        while not exit_event.is_set():
            try:
                await asyncio.wait_for(exit_event.wait(), timeout=0.1)

            except asyncio.TimeoutError:
                continue  # Normal timeout, proceed to next check

        log.info("KopfResourceWatcher exiting as exit_event is set.")

    async def run_in_thread(self, stop_flag: threading.Event) -> None:
        """Run Kopf watcher in another thread.

        kopf/docs/embedding.rst: 
            Embeddable kopf with other application, run Kopf is to provide an event-loop in a separate thread.
        """
        await asyncio.to_thread(kopf.run,
                                stop_flag=stop_flag,
                                namespaces=[CONF.namespace])


class SimpleFileWatcher(ResourceWatcher):
    """A Simple File-based Watcher"""

    def __init__(self) -> None:
        self.chan: Channel
        self.files: List[str] = CONF.file
        self.caches: Dict[str, Cache] = {}
        super().__init__()

    async def run(self, channel: Channel, exit_event: asyncio.Event) -> None:
        """Asynchronously watch files and notify changes via channel."""
        self.chan = channel

        def select_port() -> int:
            """Select a minimun unused port."""
            _used_set = set([int(cache.port)
                            for cache in self.caches.values()])
            _port = CONF.port_min
            while _port <= CONF.port_max:
                if _port not in _used_set:
                    return _port
                _port += 1
            raise RuntimeError(
                f"No available port in range [{CONF.port_min}, {CONF.port_max}]")

        while not exit_event.is_set():
            try:
                for file in self.files:
                    cache = self.caches.get(file, None)
                    log.debug(f"On watching file {file} with cache {cache}...")

                    if not FileTool.validate(file):
                        log.error(f"On watching file {file}, validation fail")
                        continue

                    mtime = FileTool.mtime(
                        file, cache.mtime if cache else None)
                    if not mtime:
                        log.debug(f"On watching file {file}, content not modified")
                        continue

                    port = cache.port if cache else str(select_port())
                    openapi_data = json.loads(FileTool.read(file))
                    resource = Resource(uid=mtime,
                                        name=file,
                                        data=openapi_data,
                                        label=port,
                                        action=ACTION_MODIFY if file in self.caches else ACTION_ADD)
                    await self.chan.send(resource.name, resource)

                    log.info(
                        f"A resource [uid: {mtime} | name: {file}] successfully sent to channel.")
                    self.caches[file] = Cache(mtime=mtime, port=port)

                await asyncio.wait_for(exit_event.wait(), timeout=3)

            except asyncio.TimeoutError:
                continue  # Normal timeout, proceed to next check
            except Exception as e:
                log.error(
                    f"Woops! unexpected error while watching files: {e}", exc_info=True)

        log.info("SimpleFileWatcher exiting as exit_event is set.")

    async def run_in_thread(self, stop_flag: threading.Event) -> None:
        """File watcher does not need a separate thread."""
        # TODO, or maybe a large file need to be watched in another thread?
        await asyncio.sleep(0.1)


resource_watcher = KopfResourceWatcher() \
    if CONF.kopf else SimpleFileWatcher()
