#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python
import asyncio
from collections import defaultdict
from typing import cast
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
from kv import Service, kv_cas, kv_read, register_kv_messages
from maelstrom import Node, Message

node = Node()
register_kafka_messages(node)
register_kv_messages(node)

"""
Seems like the goal of this exercise is to create a multi-leader Kafka-like system.
No requirements in partition tolerance or to consider additional network latency.
We're suggested to use lin-kv as the message store. 

use locks per key to reduce CAS contention for appends
use pages so we don't have to read the whole log back for every append
messages are store in lin-kv like this:
{key}_{page_num}: [messages in page]

Results:
 :availability {:valid? true, :ok-fraction 0.99947965},
 :net {:all {:send-count 154500,
             :recv-count 154500,
             :msg-count 154500,
             :msgs-per-op 8.932185},
       :clients {:send-count 42922,
                 :recv-count 42922,
                 :msg-count 42922},
       :servers {:send-count 111578,
                 :recv-count 111578,
                 :msg-count 111578,
                 :msgs-per-op 6.450714},
       :valid? true},
 :workload {:valid? true,
            :worst-realtime-lag {:time 0.021982495,
                                 :process 1,
                                 :key "9",
                                 :lag 0.0},
            :bad-error-types (),
            :error-types (),
            :info-txn-causes ()},
 :valid? true}

With some logging, it looks like CAS contention is not too bad
~5% for both send and commit_offsets
Simply substituting lin-kv for seq-kv for send and poll made perf a little worse.
~11% fails for send

Improvements:
Didn't take advantage of the fact that sent messages don't have a recency requirement.
As long as it's consistent, a message doesn't have to appear in a poll immediately after
being written. This suggests using seq-kv for message storage.
Can use hashing to consistently pick writer per key.
The node could also hold state about the keys it owns. This would also allow batching writes.
Offset metadata should still go to lin-kv
"""
PAGE_SIZE = 20
locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

async def get_current_page(key: str):
    page_key = f'{key}_current_page'
    try:
        return cast(int, await kv_read(node, Service.LinKV, key=page_key))
    except KeyError:
        # we can create the key in a later step
        return -1

async def update_current_page(key: str, prev_page: int, current_page: int):
    page_key = f'{key}_current_page'
    await kv_cas(node, Service.LinKV,
                 key=page_key, from_=prev_page, to=current_page, create_if_not_exists=True)

async def add_new_page(key: str, page_n: int, message: int):
    await kv_cas(node, Service.LinKV,
                 key=f'{key}_{page_n}', from_='', to=[message],
                 create_if_not_exists=True)

async def append_message(key: str, page_n: int, message: int):
    """Returns (page number, position) message was appended to. Throws ValueError if CAS failed"""
    if page_n < 0:
        await add_new_page(key, 0, message)
        return (0, 0)
    else:
        page: list[int] = await kv_read(node, Service.LinKV, key=f'{key}_{page_n}')
        if len(page) == PAGE_SIZE:
            await add_new_page(key, page_n + 1, message)
            return (page_n + 1, 0)
        else:
            await kv_cas(node, Service.LinKV,
                         key=f'{key}_{page_n}', from_=page, to=page + [message])
            return (page_n, len(page))

send_count = 0
send_cas_fails = 0
@node.handler('send')
async def handle_send(send_msg: Message[SendMB]):
    global send_count, send_cas_fails
    send_count += 1
    key = send_msg.body.key
    message = send_msg.body.msg
    lock = locks[key]
    async with lock:
        while True:
            current_page = await get_current_page(key)
            try:
                appended_page, position = await append_message(key, current_page, message)
            except ValueError:
                # failed to add message. Should retry from top
                send_cas_fails += 1
                continue
            if current_page != appended_page:
                # what if another node successfully appends to page at same time?
                # this should be fine since this append was successful. the other node
                # should have advanced the current page correctly
                try:
                    await update_current_page(key, current_page, appended_page)
                except ValueError:
                    pass
            break

    node.log(f'send CAS fail ratio: {send_cas_fails / send_count}')
    offset = (PAGE_SIZE * appended_page) + position
    await node.reply(send_msg, SendReplyMB(offset=offset))

@node.handler('poll')
async def handle_poll(poll_msg: Message[PollMB]):
    poll_reply = {}
    for key, offset in poll_msg.body.offsets.items():
        try:
            page: list[int] = await kv_read(node, Service.LinKV, key=f'{key}_{offset // PAGE_SIZE}')
            poll_reply[key] = [[offset + i, msg] for i, msg in enumerate(page[offset % PAGE_SIZE:])]
        except KeyError:
            # omit this key from the response
            continue 
          
    await node.reply(poll_msg, PollReplyMB(msgs=poll_reply))

commit_offsets_count = 0
commit_cas_fails = 0
@node.handler('commit_offsets')
async def handle_commit_offsets(commit_offsets_msg: Message[CommitOffsetsMB]):
    global commit_offsets_count, commit_cas_fails
    commit_offsets_count += 1
    for key, offset in commit_offsets_msg.body.offsets.items():
        commit_key = f'{key}_committed_offset'
        while True:
            try:
                committed_offset: int = await kv_read(node, Service.LinKV, key=commit_key)
            except KeyError:
                # does not exist yet. this is fine
                committed_offset = -1
            try:
                await kv_cas(node, Service.LinKV,
                             key=commit_key, from_=committed_offset, to=offset, create_if_not_exists=True)
                break
            except ValueError:
                # retry the offset commit
                commit_cas_fails += 1
                continue

    node.log(f'commit_offsets CAS fail ratio: {commit_cas_fails / commit_offsets_count}')
    await node.reply(commit_offsets_msg, CommitOffsetsReplyMB())

@node.handler('list_committed_offsets')
async def handle_list_committed_offsets(list_committed_offsets_msg: Message[ListCommittedOffsetsMB]):
    list_reply: dict[str, int] = {}
    for key in list_committed_offsets_msg.body.keys:
        commit_key = f'{key}_committed_offset'
        try:
            list_reply[key]: int = await kv_read(node, Service.LinKV, key=commit_key)
            break
        except KeyError:
            # omit this key from the response
            continue
    await node.reply(list_committed_offsets_msg, ListCommittedOffsetsReplyMB(offsets=list_reply))

node.run()