#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

from dataclasses import dataclass
from itertools import count
from maelstrom import Node, Message, MessageBody
    
node = Node()

@node.message('generate')
@dataclass
class GenerateMessageBody(MessageBody):
    pass

@dataclass(kw_only=True)
class GenerateReplyMessageBody(MessageBody):
    type: str = 'generate_ok'
    id: str

counter = count()

@node.handler('generate')
async def handle_generate(generate_msg: Message[GenerateMessageBody]):
    generate_reply = GenerateReplyMessageBody(id=f'{node.id}_{next(counter)}')
    await node.reply(generate_msg, generate_reply)

node.run()