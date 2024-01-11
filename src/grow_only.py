#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

import asyncio
import random
from dataclasses import dataclass
from typing import cast
from kv import KVCASMessageBody, KVReadMessageBody, Service, handle_kv_cas_reply, handle_kv_read_reply, register_kv_messages
from maelstrom import Node, Message, MessageBody

node = Node()
register_kv_messages(node)

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

async def read_with_default(key: str):
    try:
        return cast(int, await node.rpc(Service.SeqKV, KVReadMessageBody(key=key),
                                        handle_kv_read_reply, retry_timeout=random.random))
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
            cas_msg = KVCASMessageBody(key=node.id,
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
            await node.rpc(Service.SeqKV, cas_msg, handle_kv_cas_reply)
            break
        except ValueError:
            continue
    await node.reply(add_msg, AddReplyMessageBody())

@node.handler('read')
async def handle_read(read_msg: Message[ReadMessageBody]):
    vals = await asyncio.gather(*(read_with_default(node_id) for node_id in node.node_ids))
    # seq-kv is sequentially consistent, so a read on a different node's key can be stale.
    # Adding a second read seems to be enough to ensure freshness in this case, but
    # another option is to ask other nodes for their values. They are the clients
    # responsible for updating those keys, so seq-kv should always return the current
    # value. Maybe add a backup to seq-kv and add a timeout in case of network partition
    vals2 = await asyncio.gather(*(read_with_default(node_id) for node_id in node.node_ids))
    await node.log(f'Current known vals: {vals}')
    await node.reply(read_msg, ReadReplyMessageBody(value=sum(max(p) for p in zip(vals, vals2))))

node.run()