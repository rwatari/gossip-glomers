#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

import asyncio
from dataclasses import dataclass
import random
from maelstrom import Node, Message, MessageBody
    
node = Node()

@dataclass(kw_only=True)
class BroadcastMessageBody(MessageBody):
    type: str = 'broadcast'
    message: int

@dataclass
class BroadcastReplyMessageBody(MessageBody):
    type: str = 'broadcast_ok'

@dataclass
class ReadMessageBody(MessageBody):
    type: str = 'read'

@dataclass(kw_only=True)
class ReadReplyMessageBody(MessageBody):
    type: str = 'read_ok'
    messages: list[int]

@dataclass(kw_only=True)
class TopologyMessageBody(MessageBody):
    type: str = 'topology'
    topology: dict[str, list[str]]

@dataclass
class TopologyReplyMessageBody(MessageBody):
    type: str = 'topology_ok'

@dataclass(kw_only=True)
class PropagateMessageBody(MessageBody):
    type: str = 'propagate'
    message: int

@dataclass
class PropagateReplyMessageBody(MessageBody):
    type: str = 'propagate_ok'

seen_messages = set()
neighbors: list[str] = []

"""
The challenge is not too clear on what kind of solutions are allowed. For the Efficiency
parts, it seems to be impossible to achieve the goal metrics without using our own node
topology (spanning tree) or batching messages.
"""

@node.handler(BroadcastMessageBody)
async def handle_broadcast(broadcast_msg: Message[BroadcastMessageBody]):
    seen_messages.add(broadcast_msg.body.message)
    await node.reply(broadcast_msg, BroadcastReplyMessageBody())
    propagate_msg = PropagateMessageBody(message=broadcast_msg.body.message)
    await propagate_with_retries(neighbors, propagate_msg)

@node.handler(PropagateReplyMessageBody)
async def handle_propagate_ok(propagate_ok_msg: Message[PropagateReplyMessageBody]):
    # this can be anything since we only need to verify the future completed
    assert propagate_ok_msg.body.type == 'propagate_ok'

async def propagate_with_retries(dests: list[str], propagate_msg: PropagateMessageBody):
    await asyncio.gather(*(node.rpc(dest, propagate_msg, handle_propagate_ok,
                                    retry_timeout=random.random) for dest in dests))

@node.handler(ReadMessageBody)
async def handle_read(read_msg: Message[ReadMessageBody]):
    read_reply = ReadReplyMessageBody(messages=list(seen_messages))
    await node.reply(read_msg, read_reply)

@node.handler(TopologyMessageBody)
async def handle_topology(topology_msg: Message[TopologyMessageBody]):
    global neighbors
    neighbors = topology_msg.body.topology[node.id]
    await node.reply(topology_msg, TopologyReplyMessageBody())

@node.handler(PropagateMessageBody)
async def handle_propagate(propagate_msg: Message[PropagateMessageBody]):
    await node.reply(propagate_msg, PropagateReplyMessageBody())
    if propagate_msg.body.message not in seen_messages:
        seen_messages.add(propagate_msg.body.message)
        await propagate_with_retries([n for n in neighbors if n != propagate_msg.src], propagate_msg.body)

node.run()