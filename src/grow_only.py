#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

import asyncio
import random
from dataclasses import dataclass
from typing import Any, cast
from maelstrom import ErrorMessageBody, Node, Message, MessageBody

node = Node()

### seq-kv messages ###############################
@dataclass(kw_only=True)
class SeqKVReadMessageBody(MessageBody):
    type: str = 'read'
    key: Any

@node.message('read_ok')
@dataclass(kw_only=True)
class SeqKVReadReplyMessageBody(MessageBody):
    type: str = 'read_ok'
    value: Any

@dataclass(kw_only=True)
class SeqKVWriteMessageBody(MessageBody):
    type: str = 'write'
    key: Any
    value: Any

@node.message('write_ok')
@dataclass
class SeqKVWriteReplyMessageBody(MessageBody):
    type: str = 'write_ok'

@dataclass(kw_only=True)
class SeqKVCASMessageBody(MessageBody):
    type: str = 'cas'
    key: Any
    from_: Any # 'from' is a reserved word so we can't use as an attrib
    to: Any
    create_if_not_exists: bool | None = None

@node.message('cas_ok')
@dataclass
class SeqKVCASReplyMessageBody(MessageBody):
    type: str = 'cas_ok'

### grow_only ############################################
@node.message('add')
@dataclass
class AddMessageBody(MessageBody):
    delta: int

@dataclass
class AddReplyMessageBody(MessageBody):
    type: str = 'add_ok'

@node.message('read')
@dataclass
class ReadMessageBody(MessageBody):
    pass

@dataclass(kw_only=True)
class ReadReplyMessageBody(MessageBody):
    type: str = 'read_ok'
    value: int

async def handle_read_reply(read_reply_msg: Message[SeqKVReadReplyMessageBody | ErrorMessageBody]):
    match read_reply_msg.body:
        case SeqKVReadReplyMessageBody():
            return cast(int, read_reply_msg.body.value)
        case ErrorMessageBody(code=20): # key nonexistent
            raise KeyError(f'Error from seq-kv on read: {read_reply_msg}')
        case _:
            raise RuntimeError(f'Unexpected reply type from seq-kv on read: {read_reply_msg}')

async def handle_cas_reply(cas_reply_msg: Message[SeqKVCASReplyMessageBody | ErrorMessageBody]):
    match cas_reply_msg.body:
        case SeqKVCASReplyMessageBody():
            return
        case ErrorMessageBody(code=22): # inconsistent state
            raise ValueError(f'CAS failed due to bad "from" value: {cas_reply_msg}')
        case _:
            raise RuntimeError(f'Unexpected reply type from seq-kv on CAS: {cas_reply_msg}')

async def read_with_default(key: str):
    try:
        return await node.rpc('seq-kv', SeqKVReadMessageBody(key=key),
                              handle_read_reply, retry_timeout=random.random)
    except KeyError:
        return 0

@node.handler('add')
async def handle_add(add_msg: Message[AddMessageBody]):
    # This is like the atomic int algorithm
    while True:
        try:
            prev_value = await read_with_default(node.id)
            next_value = prev_value + add_msg.body.delta

            # do cas with retry
            cas_msg = SeqKVCASMessageBody(key=node.id,
                                          from_=prev_value,
                                          to=next_value,
                                          create_if_not_exists=True)
            
            # We shouldn't retry or we can over-add
            # In this example, it seems like seq-kv is fully available so we don't
            # need to worry about retrying.
            # If there was a network issue to seq-kv, we would need to ensure only
            # one of the adds was occuring at a time and verify we can stop retrying
            # Not sure if there is a stateless solution if seq-kv isn't available.
            # If a successful CAS ack message isn't delivered to a node, no node has
            # a way of knowing why the store was incremented
            await node.rpc('seq-kv', cas_msg, handle_cas_reply)
            break
        except ValueError:
            continue
    await node.reply(add_msg, AddReplyMessageBody())

@node.handler('read')
async def handle_read(read_msg: Message[ReadMessageBody]):
    vals = await asyncio.gather(*(read_with_default(node_id) for node_id in node.node_ids))
    # The final read has a habit of desyncing. n0 could be out of date part way through reading
    # Adds a lot of reads, but is generally more successful
    vals2 = await asyncio.gather(*(read_with_default(node_id) for node_id in node.node_ids))
    await node.log(f'Current known vals: {vals}')
    await node.reply(read_msg, ReadReplyMessageBody(value=sum(max(p) for p in zip(vals, vals2))))

node.run()