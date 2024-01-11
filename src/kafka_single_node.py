#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

from collections import defaultdict
from typing import Generic, TypeVar
from kafka import (
    CommitOffsetsMB,
    CommitOffsetsReplyMB,
    ListCommittedOffsetsMB,
    ListCommittedOffsetsReplyMB,
    PollMB,
    PollReplyMB,
    SendMB,
    SendReplyMB,
    register_kafka_messages,
)
from maelstrom import Node, Message

node = Node()
register_kafka_messages(node)

T = TypeVar('T')
class AppendOnlyLog(Generic[T]):
    def __init__(self):
        self._log: list[T] = []
        self.committed_offset = -1
    
    def next_offset(self):
        return len(self._log)
    
    def append(self, message: T):
        offset = self.next_offset()
        self._log.append(message)
        return offset
    
    def poll(self, offset: int):
        return [[offset + i, msg] for i, msg in enumerate(self._log[offset:])]
    
    def commit_offset(self, offset: int):
        self.committed_offset = offset
    
logs: dict[str, AppendOnlyLog[int]] = defaultdict(AppendOnlyLog[int])

@node.handler('send')
async def handle_send(send_msg: Message[SendMB]):
    offset = logs[send_msg.body.key].append(send_msg.body.msg)
    await node.reply(send_msg, SendReplyMB(offset=offset))

@node.handler('poll')
async def handle_poll(poll_msg: Message[PollMB]):
    reply = PollReplyMB(msgs={k: logs[k].poll(offset)
                              for k, offset in poll_msg.body.offsets.items()})
    await node.reply(poll_msg, reply)

@node.handler('commit_offsets')
async def handle_commit_offsets(commit_offsets_msg: Message[CommitOffsetsMB]):
    for k, offset in commit_offsets_msg.body.offsets.items():
        logs[k].commit_offset(offset)
    await node.reply(commit_offsets_msg, CommitOffsetsReplyMB())

@node.handler('list_committed_offsets')
async def handle_list_committed_offsets(list_committed_offsets_msg: Message[ListCommittedOffsetsMB]):
    reply = ListCommittedOffsetsReplyMB(offsets={k: logs[k].committed_offset
                                                 for k in list_committed_offsets_msg.body.keys})
    await node.reply(list_committed_offsets_msg, reply)

node.run()