"""
RxPY example running a Tornado server doing search queries against Wikipedia to
populate the autocomplete dropdown in the web UI. Start using
`python autocomplete.py` and navigate your web browser to http://localhost:8080

Uses the RxPY IOLoopScheduler.
"""

import os
import asyncio
from asyncio import Future
from typing import Any, Dict, Union

import tornado
import tornado.websocket

import continuationmonad

import rxbp
from rxbp.abc import FlowableNode
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.state import State
from rxbp.typing import Flowable

loop = asyncio.new_event_loop()
scheduler= continuationmonad.init_main_asyncio_scheduler(
    loop=loop,
)



def search_wikipedia(term: str) -> Future[tornado.httpclient.HTTPResponse]:
    """Search Wikipedia for a given term"""
    url = "http://en.wikipedia.org/w/api.php"

    params: Dict[str, str] = {"action": "opensearch", "search": term, "format": "json"}
    # Must set a user agent for non-browser requests to Wikipedia
    user_agent = (
        "RxPY/3.0 (https://github.com/dbrattli/RxPY; dag@brattli.net) Tornado/4.0.1"
    )

    url = tornado.httputil.url_concat(url, params)

    http_client = tornado.httpclient.AsyncHTTPClient()
    return http_client.fetch(url, method="GET", user_agent=user_agent)



class CreateTornadoApp(FlowableNode[Flowable[None]]):
    def unsafe_subscribe(
        self, 
        state: State, 
        args: SubscribeArgs[Flowable[None]],
    ):
        class WSHandler(tornado.websocket.WebSocketHandler):
            def open(self, *_):
                # print("WebSocket opened")

                def create_handler(inner_observer, _):
                    pass

                args.observer.on_next(rxbp.create(create_handler))

            def on_message(self, message: Union[bytes, str]):
                obj = tornado.escape.json_decode(message)
                self.subject.on_next(obj)

            def on_close(self):
                print("WebSocket closed")

        class MainHandler(tornado.web.RequestHandler):
            def get(self):
                self.render("index.html")

        async def main():
            app = tornado.web.Application(
                [
                    tornado.web.url(r"/", MainHandler),
                    (r"/ws", WSHandler),
                    (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": "."}),
                ]
            )
            app.listen(8888)

        loop.create_task(main())

        return state, rxbp.init_subscription_result(
            certificate=scheduler._create_certificate(weight=args.weight, stack=tuple()),
            cancellable=None,
        )



def create_tornado_app(observer, _):

    class WSHandler(tornado.websocket.WebSocketHandler):
        def open(self, *args: Any):
            # print("WebSocket opened")

            def create_handler(inner_observer, _):
                pass

            observer.on_next(rxbp.create(create_handler))

        def on_message(self, message: Union[bytes, str]):
            obj = tornado.escape.json_decode(message)
            self.subject.on_next(obj)

        def on_close(self):
            print("WebSocket closed")

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            self.render("index.html")

    async def main():
        app = tornado.web.Application(
            [
                tornado.web.url(r"/", MainHandler),
                (r"/ws", WSHandler),
                (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": "."}),
            ]
        )
        app.listen(8888)

    loop.create_task(main())
    return continuationmonad.from_(scheduler._create_certificate(weight=1, stack=tuple()))


rxbp.create(create_tornado_app)



class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


# async def main():
#     app = make_app()
#     app.listen(8888)

# loop.create_task(main())
scheduler.start_loop()
