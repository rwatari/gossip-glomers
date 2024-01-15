#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

from dataclasses import dataclass
from maelstrom import Node, Message, MessageBody

node = Node()


@dataclass(kw_only=True)
class EchoMessageBody(MessageBody):
    type: str = "echo"
    echo: str


@dataclass(kw_only=True)
class EchoReplyMessageBody(MessageBody):
    type: str = "echo_ok"
    echo: str


@node.handler(EchoMessageBody)
async def handle_echo(echo_msg: Message[EchoMessageBody]):
    echo_reply = EchoReplyMessageBody(echo=echo_msg.body.echo)
    await node.reply(echo_msg, echo_reply)


node.run()
