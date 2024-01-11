from dataclasses import dataclass
from maelstrom import MessageBody, Node

@dataclass
class SendMB(MessageBody):
    key: str
    msg: int

@dataclass(kw_only=True)
class SendReplyMB(MessageBody):
    type: str = 'send_ok'
    offset: int

@dataclass
class PollMB(MessageBody):
    offsets: dict[str, int]

@dataclass(kw_only=True)
class PollReplyMB(MessageBody):
    type: str = 'poll_ok'
    msgs: dict[str, list[list[int]]]

@dataclass
class CommitOffsetsMB(MessageBody):
    offsets: dict[str, int]

@dataclass
class CommitOffsetsReplyMB(MessageBody):
    type: str = 'commit_offsets_ok'

@dataclass
class ListCommittedOffsetsMB(MessageBody):
    keys: list[str]

@dataclass(kw_only=True)
class ListCommittedOffsetsReplyMB(MessageBody):
    type: str = 'list_committed_offsets_ok'
    offsets: dict[str, int]

def register_kafka_messages(node: Node):
    node.message('send')(SendMB)
    node.message('poll')(PollMB)
    node.message('commit_offsets')(CommitOffsetsMB)
    node.message('list_committed_offsets_ok')(ListCommittedOffsetsMB)