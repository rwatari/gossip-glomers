from dataclasses import dataclass
from maelstrom import MessageBody


@dataclass(kw_only=True)
class SendMB(MessageBody):
    type: str = "send"
    key: str
    msg: int


@dataclass(kw_only=True)
class SendReplyMB(MessageBody):
    type: str = "send_ok"
    offset: int


@dataclass(kw_only=True)
class PollMB(MessageBody):
    type: str = "poll"
    offsets: dict[str, int]


@dataclass(kw_only=True)
class PollReplyMB(MessageBody):
    type: str = "poll_ok"
    msgs: dict[str, list[list[int]]]


@dataclass(kw_only=True)
class CommitOffsetsMB(MessageBody):
    type: str = "commit_offsets"
    offsets: dict[str, int]


@dataclass
class CommitOffsetsReplyMB(MessageBody):
    type: str = "commit_offsets_ok"


@dataclass(kw_only=True)
class ListCommittedOffsetsMB(MessageBody):
    type: str = "list_committed_offsets"
    keys: list[str]


@dataclass(kw_only=True)
class ListCommittedOffsetsReplyMB(MessageBody):
    type: str = "list_committed_offsets_ok"
    offsets: dict[str, int]
