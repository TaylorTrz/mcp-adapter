import anyio
import asyncio
import logging
import signal
import threading
import uvicorn

from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from mcp_adapter import tool
from mcp_adapter import server
from mcp_adapter import openapi
from mcp_adapter import watcher


CONF = tool.config.conf
log = logging.getLogger(tool.APP)


@asynccontextmanager
async def lifespan(app):
    log.info("Adapter http server is starting...")
    yield
    log.warning('Adapter http server is shutting down...')


async def health(request) -> JSONResponse:
    """Health check
    
    todo: (taylor), add introspection data to review server internal state
    """
    return JSONResponse(
        {"code": "200", "msg": "ok", "data": {}}
    )


async def run_http_server():
    routes = [
        Route("/health", health, methods=['GET'], name="health"),
    ]
    app = Starlette(debug=True, routes=routes, lifespan=lifespan)
    config = uvicorn.Config(app=app,
                            host=CONF.host,
                            port=int(CONF.port),
                            log_config=tool.LOGGING_CONFIG,
                            log_level=CONF.log_level.lower())


    server = uvicorn.Server(config)
    await server.serve()
    

class MCPAdapter(object):
    """A main loop to manager all async tasks
    
    Behold, a simple and useful adapter tool is born!

    For many stale http servers, mcp-adapter can help convert them into a
    completed mcp application automatically. All we need is a valid OpenAPI
    specification json-like document, then mcp-adapter will load and parse it,
    and convert all http apis into mcp apis, finally serve them via mcp protocol.
    """

    def __init__(self):
        self.introspection = {}
        self.exit_event = asyncio.Event()
        self.thread_exit_event = threading.Event()
        return super().__init__()
    
    @property
    def event_loop(self):
        return asyncio.get_event_loop()
    
    @property
    def signal_set(self) -> bool:
        return self.exit_event.is_set()

    async def main(self):
        """Raise all async tasks and wait for a stop signal.
        
        Since more and more python modules handle async task with async/await,
        like FastMCP and Kopf. We select python coroutine model to better coperate
        with them, and alll async tasks are managed in this main loop.
        
        Meanwhile, python thread model is also used to run Kopf watcher, since Kopf
        needs to run in a clean context. A threading.Event is used to notify Kopf
        to stop when main loop is going to exit.
        """
        log.info("App (%s) is starting with options (%s)", CONF.name, CONF)
        
        def _handle_signal(signo, frame):
            # NOTE: call_soon_threadsafe is a must to avoid a hanging coroutine
            self.event_loop.call_soon_threadsafe(self.exit_event.set)
            self.event_loop.call_soon_threadsafe(self.thread_exit_event.set)
            log.info("Handler receive signal %s is_set=%s", str(signo), self.signal_set)
        
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)
        
        watcher_chan = tool.Channel("resource", asyncio.Queue())
        openapi_chan = tool.Channel("openapi", asyncio.Queue())

        async with anyio.create_task_group() as tg:
            # A resoure watcher task will notify each resource change event via channel
            tg.start_soon(watcher.resource_watcher.run, watcher_chan, self.exit_event)
            tg.start_soon(watcher.resource_watcher.run_in_thread, self.thread_exit_event)

            # A openapi loader task will receive resource event and process openapi spec via another channel
            tg.start_soon(openapi.openapi_loader.run, watcher_chan, openapi_chan, self.exit_event)

            # A mcp server task will receive openapi spec and convert them into multiply mcp servers
            tg.start_soon(server.mcp_manager.run, openapi_chan, self.exit_event)

            # A http server task to provide health check, instrospection api etc.
            tg.start_soon(run_http_server)
            log.info("App (%s) started, converting HTTP to MCP asyncronously...", CONF.name)

            await self.exit_event.wait()
            
        log.warning("App (%s) gracefully shutdown.", CONF.name)
    
    
def main():
    anyio.run(MCPAdapter().main)

    
if __name__ == "__main__":
    main()