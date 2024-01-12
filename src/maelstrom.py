import asyncio
import json
import signal
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, asdict, replace
from itertools import count
from typing import Protocol, TypeVar, Generic, Any


@dataclass(kw_only=True)
class MessageBody:
    type: str
    msg_id: int | None = None
    in_reply_to: int | None = None


MessageBodyT = TypeVar("MessageBodyT", bound=MessageBody)
MessageBodyS = TypeVar("MessageBodyS", bound=MessageBody)


@dataclass
class Message(Generic[MessageBodyT]):
    src: str
    dest: str
    body: MessageBodyT

    @classmethod
    def from_dict(
        cls, body_factory: Callable[[dict], MessageBodyT], data_dict: dict[str, Any]
    ):
        """
        Usage: Message[SubclassMessageBody].from_dict(SubclassMessageBody, data_dict)
        """
        body = body_factory(**data_dict["body"])
        return cls(data_dict["src"], data_dict["dest"], body)

    @classmethod
    def from_json(cls, body_factory: Callable[[dict], MessageBodyT], data: str):
        """
        Usage: Message[SubclassMessageBody].from_json(SubclassMessageBody, data)
        """
        data_dict = json.loads(data)
        return cls.from_dict(body_factory, data_dict)

    def to_json(self) -> str:
        # use rstrip for reserved word attributes that need to be converted like "from"
        body = {k.rstrip("_"): v for k, v in asdict(self.body).items() if v is not None}
        return json.dumps({"src": self.src, "dest": self.dest, "body": body})


@dataclass(kw_only=True)
class InitMessageBody(MessageBody):
    type: str = "init"
    node_id: str
    node_ids: list[str]


@dataclass(kw_only=True)
class ErrorMessageBody(MessageBody):
    type: str = "error"
    code: int
    text: str


@dataclass(kw_only=True)
class InitReplyMessageBody(MessageBody):
    type: str = "init_ok"


T = TypeVar("T")


class MessageHandler(Protocol[MessageBodyT, T]):
    def __call__(self, message: Message[MessageBodyT]) -> Awaitable[T]:
        ...


RetryTimeout = Callable[[], float] | float | None

class Node:
    def __init__(self):
        self.id: str | None = None
        self.node_ids: list[str] = []
        self.message_counter = count()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._message_constructors: dict[str, Callable[[dict], Message]] = {}
        self._handlers: dict[str, MessageHandler] = {}
        self._callbacks: dict[int, MessageHandler] = {}
        self._register_init()

    ### Decorators for message and handler registration
    def message(self, msg_type: str):
        """
        Every message type that a node can receive should be registered with this
        decorator.
        """
        def wrapper(msg_constructor: MessageBodyT):
            if msg_type in self._message_constructors:
                raise ValueError(f"Message type '{msg_type}' is already registered")
            self._message_constructors[msg_type] = lambda data_dict: Message[
                MessageBodyT
            ].from_dict(msg_constructor, data_dict)
            return msg_constructor

        return wrapper

    # TODO: handler messages probably don't need the message container and should
    #       reply with the return value
    # TODO: can register the message constructor in the same decorator call as a 2nd arg
    def handler(self, msg_type: str):
        def wrapper(handler_func: MessageHandler):
            self._handlers[msg_type] = handler_func
            return handler_func

        return wrapper

    ### Public methods
    def run(self):
        asyncio.run(self._run())

    def log(self, log_msg: str):
        print(log_msg, file=sys.stderr, flush=True)

    async def send(self, dest: str, message_body: MessageBodyT):
        message = Message[MessageBodyT](src=self.id, dest=dest, body=message_body)
        message_str = message.to_json() + "\n"
        self._writer.write(message_str.encode())
        await self._writer.drain()

    async def reply(
        self, original_msg: Message[MessageBodyT], reply_body: MessageBodyS
    ):
        reply_body = replace(reply_body, in_reply_to=original_msg.body.msg_id)
        await self.send(original_msg.src, reply_body)

    async def rpc(
        self,
        dest: str,
        message_body: MessageBodyT,
        callback: MessageHandler[MessageBodyS, T],
        retry_timeout: RetryTimeout = None,
    ) -> T:
        """
        retry_timeout is in seconds and can be a function. Will not retry if None
        An exception thrown in the callback will be available to the caller of Node.rpc()
        """
        loop = asyncio.get_running_loop()
        while True:
            callback_result: asyncio.Future[T] = loop.create_future()

            async def wrapped_cb(cb_msg_arg: Message[MessageBodyS]):
                try:
                    callback_result.set_result(await callback(cb_msg_arg))
                except asyncio.InvalidStateError:
                    self.log("Tried to set callback result on a cancelled future")
                except Exception as e:
                    callback_result.set_exception(e)

            msg_id = await self._rpc(dest, message_body, wrapped_cb)
            try:
                t = retry_timeout() if callable(retry_timeout) else retry_timeout
                async with asyncio.timeout(t):
                    return await callback_result
            except TimeoutError:
                callback_result.cancel()
                self._callbacks.pop(msg_id, None)
                self.log(
                    f"Timeout while waiting for reply from {dest} on msg_id {msg_id}"
                )

    async def _run(self):
        await self._init_stream()
        loop = asyncio.get_running_loop()
        try:
            while True:
                message_json = (await self._reader.readline()).strip()
                loop.create_task(self._handle_message(message_json))
        except asyncio.CancelledError:
            print("Node killed", file=sys.stderr)

    def _handle_sig(self):
        tasks = asyncio.all_tasks()
        for task in tasks:
            task.cancel()

    async def _init_stream(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_sig)
        self._reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(self._reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        w_transport, w_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout
        )
        self._writer = asyncio.StreamWriter(w_transport, w_protocol, None, loop)

    # TODO: Maybe messages can be namespaced or matched to source
    # Create namespace for maelstrom services like lin-kv, seq-kv
    # If source is in node_ids, it's an inter-node message type
    # API space for clients
    async def _handle_message(self, message_json: str):
        msg_obj = json.loads(message_json)
        msg_type = msg_obj["body"]["type"]
        msg = self._message_constructors[msg_type](msg_obj)
        if msg.body.in_reply_to is not None:
            try:
                callback = self._callbacks.pop(msg.body.in_reply_to)
                await callback(msg)
            except KeyError:
                self.log(f"No callback for message {msg}")
        else:
            await self._handlers[msg_type](msg)

    async def _rpc(
        self, dest: str, message_body: MessageBodyT, handler: MessageHandler
    ) -> int:
        # this should be ok since we're not yielding control until await right?
        msg_id = next(self.message_counter)
        self._callbacks[msg_id] = handler
        message_body = replace(message_body, msg_id=msg_id)
        await self.send(dest, message_body)
        return msg_id

    def _register_init(self):
        self.message("init")(InitMessageBody)
        self.handler("init")(self._handle_init)
        self.message("error")(ErrorMessageBody)

    async def _handle_init(self, init_msg: Message[InitMessageBody]):
        self.id = init_msg.body.node_id
        self.node_ids = init_msg.body.node_ids
        self.log(f"Initialized node {self.id}")
        await self.reply(init_msg, InitReplyMessageBody())
