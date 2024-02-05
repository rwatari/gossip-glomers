import asyncio
import json
import signal
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, asdict, replace
from itertools import count
from typing import Coroutine, TypeVar, Generic, Any


@dataclass(kw_only=True)
class MessageBody:
    type: str
    msg_id: int | None = None
    in_reply_to: int | None = None


MessageBodyT = TypeVar("MessageBodyT", bound=MessageBody, covariant=True)


@dataclass
class Message(Generic[MessageBodyT]):
    src: str
    dest: str
    body: MessageBodyT

    @classmethod
    def from_dict(cls, body_factory: type[MessageBody], data_dict: dict[str, Any]):
        """
        Usage: Message[SubclassMessageBody].from_dict(SubclassMessageBody, data_dict)
        """
        body = body_factory(**data_dict["body"])
        return cls(data_dict["src"], data_dict["dest"], body)

    @classmethod
    def from_json(cls, body_factory: type[MessageBody], data: str):
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


T = TypeVar("T", covariant=True)


MessageHandler = Callable[[Message[MessageBodyT]], Awaitable[T]]


RetryTimeout = Callable[[], float] | float | None


class Node:
    def __init__(self):
        self.id: str
        self.node_ids: list[str] = []
        self.message_counter = count()
        self._reader: asyncio.StreamReader
        self._writer: asyncio.StreamWriter
        self._message_constructors: dict[
            str, Callable[[dict[str, Any]], Message[Any]]
        ] = {}
        self._handlers: dict[str, MessageHandler[Any, Any]] = {}
        self._callbacks: dict[int, MessageHandler[Any, Any]] = {}
        self._background_task: asyncio.Task[Any]
        self._register_init()

    def register_message_type(self, msg_type: type[MessageBody]):
        if msg_type.type in self._message_constructors:
            raise ValueError(f"Message type '{msg_type.type}' is already registered")
        self._message_constructors[msg_type.type] = lambda data_dict: Message[
            MessageBody
        ].from_dict(msg_type, data_dict)

    ### Decorator for message and handler registration
    # TODO: handler messages probably don't need the message container and should
    #       reply with the return value
    # TODO: handler functions should probably take the node as an argument for
    #       testing purposes
    def handler(self, msg_type: type[MessageBody]):
        def wrapper(handler_func: MessageHandler[MessageBodyT, T]):
            self.register_message_type(msg_type)
            self._handlers[msg_type.type] = handler_func
            return handler_func

        return wrapper

    ### Public methods
    def run(self, background_task: Coroutine[Any, Any, None] | None = None):
        asyncio.run(self._run(background_task))

    def log(self, log_msg: str):
        print(log_msg, file=sys.stderr, flush=True)

    async def send(self, dest: str, message_body: MessageBody):
        message = Message(src=self.id, dest=dest, body=message_body)
        message_str = message.to_json() + "\n"
        self._writer.write(message_str.encode())
        await self._writer.drain()

    async def reply(self, original_msg: Message[MessageBodyT], reply_body: MessageBody):
        reply_body = replace(reply_body, in_reply_to=original_msg.body.msg_id)
        await self.send(original_msg.src, reply_body)

    async def rpc(
        self,
        dest: str,
        message_body: MessageBody,
        callback: MessageHandler[MessageBodyT, T],
        retry_timeout: RetryTimeout = None,
    ) -> T:
        """
        retry_timeout is in seconds and can be a function. Will not retry if None
        An exception thrown in the callback will be available to the caller of Node.rpc()
        """
        loop = asyncio.get_running_loop()
        while True:
            callback_result: asyncio.Future[T] = loop.create_future()

            async def wrapped_cb(msg: Message[MessageBodyT]):
                try:
                    callback_result.set_result(await callback(msg))
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

    async def _run(self, background_task: Coroutine[Any, Any, None] | None):
        await self._init_stream()
        if background_task is not None:
            self._background_task = asyncio.create_task(background_task)
        try:
            async with asyncio.TaskGroup() as tg:
                while True:
                    message_json = (await self._reader.readline()).strip()
                    tg.create_task(self._handle_message(message_json))
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
    async def _handle_message(self, message_json: bytes):
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
        self, dest: str, message_body: MessageBody, handler: MessageHandler[Any, Any]
    ) -> int:
        # this should be ok since we're not yielding control until await right?
        msg_id = next(self.message_counter)
        self._callbacks[msg_id] = handler
        message_body = replace(message_body, msg_id=msg_id)
        await self.send(dest, message_body)
        return msg_id

    def _register_init(self):
        self.handler(InitMessageBody)(self._handle_init)
        self.register_message_type(ErrorMessageBody)

    async def _handle_init(self, init_msg: Message[InitMessageBody]):
        self.id = init_msg.body.node_id
        self.node_ids = init_msg.body.node_ids
        self.log(f"Initialized node {self.id}")
        await self.reply(init_msg, InitReplyMessageBody())
