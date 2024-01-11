from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from maelstrom import ErrorMessageBody, Message, MessageBody, Node

class Service(StrEnum):
    LinKV = 'lin-kv'
    SeqKV = 'seq-kv'
    LWWKV = 'lww-kv'

@dataclass(kw_only=True)
class KVReadMessageBody(MessageBody):
    type: str = 'read'
    key: Any

@dataclass(kw_only=True)
class KVReadReplyMessageBody(MessageBody):
    type: str = 'read_ok'
    value: Any

@dataclass(kw_only=True)
class KVWriteMessageBody(MessageBody):
    type: str = 'write'
    key: Any
    value: Any

@dataclass
class KVWriteReplyMessageBody(MessageBody):
    type: str = 'write_ok'

@dataclass(kw_only=True)
class KVCASMessageBody(MessageBody):
    type: str = 'cas'
    key: Any
    from_: Any # 'from' is a reserved word so we can't use as an attrib
    to: Any
    create_if_not_exists: bool | None = None

@dataclass
class KVCASReplyMessageBody(MessageBody):
    type: str = 'cas_ok'

# TODO: Maybe there's a more pythonic way of handling this. Mixins?
# And maybe hijack the message handler so the message types can be namespaced?
def register_kv_messages(node: Node):
    node.message('read_ok')(KVReadReplyMessageBody)
    node.message('write_ok')(KVWriteReplyMessageBody)
    node.message('cas_ok')(KVCASReplyMessageBody)

async def handle_kv_read_reply(read_reply_msg: Message[KVReadReplyMessageBody | ErrorMessageBody]):
    match read_reply_msg.body:
        case KVReadReplyMessageBody():
            return read_reply_msg.body.value
        case ErrorMessageBody(code=20): # key nonexistent
            raise KeyError(f'Error from {read_reply_msg.src} on read: {read_reply_msg}')
        case _:
            raise RuntimeError(f'Unexpected reply type from {read_reply_msg.src} on read: {read_reply_msg}')


async def handle_kv_cas_reply(cas_reply_msg: Message[KVCASReplyMessageBody | ErrorMessageBody]):
    match cas_reply_msg.body:
        case KVCASReplyMessageBody():
            return
        case ErrorMessageBody(code=20): # key nonexistent
            raise KeyError(f'Error from {cas_reply_msg.src} on read: {cas_reply_msg}')
        case ErrorMessageBody(code=22): # inconsistent state
            raise ValueError(f'CAS failed due to bad "from" value: {cas_reply_msg}')
        case _:
            raise RuntimeError(f'Unexpected reply type from {cas_reply_msg.src} on CAS: {cas_reply_msg}')