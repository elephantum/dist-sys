#!/bin/env python

import asyncio
from typing import Dict, Set

from mstrm import Message, Mstrm


class Node:
    def __init__(self, node_ids: list[str]) -> None:
        self.node_ids = node_ids

        self.known_messages: Set[int] = set()

        # Node ID -> Set of pending messages (ints)
        self.pending_broadcasts: Dict[str, Set[int]] = {
            node: set() for node in self.node_ids
        }

        self.pending_tasks: Set[asyncio.Task] = set()

    async def batch_broadcast_to_all(self, m: Mstrm) -> None:
        async def _batch_broadcast_one(node: str, messages: list[int]) -> None:
            if messages:
                await m.call(node, {"type": "batch_broadcast", "messages": messages})
                self.pending_broadcasts[node].difference_update(messages)

        tasks = [
            asyncio.create_task(
                _batch_broadcast_one(node, list(self.pending_broadcasts[node]))
            )
            for node in self.node_ids
        ]

        await asyncio.gather(*tasks)

    async def heartbeat(self, m: Mstrm, delay: float) -> None:
        while True:
            await asyncio.sleep(delay)

            task = asyncio.create_task(self.batch_broadcast_to_all(m))
            self.pending_tasks.add(task)
            task.add_done_callback(self.pending_tasks.remove)

    async def on_broadcast(self, m: Mstrm, msg: Message) -> None:
        message = msg.body["message"]
        self.known_messages.add(message)

        # add message to pending broadcasts
        for node in self.node_ids:
            self.pending_broadcasts[node].add(message)

        await m.reply(msg, {"type": "broadcast_ok"})

    async def on_batch_broadcast(self, m: Mstrm, msg: Message) -> None:
        messages = msg.body["messages"]
        self.known_messages.update(messages)

        await m.reply(msg, {"type": "batch_broadcast_ok"})

    async def on_read(self, m: Mstrm, msg: Message) -> None:
        await m.reply(msg, {"type": "read_ok", "messages": list(self.known_messages)})


async def main() -> None:
    m = await Mstrm.create()

    node = Node(m.node_ids)

    m.on("broadcast", node.on_broadcast)
    m.on("batch_broadcast", node.on_batch_broadcast)
    m.on("read", node.on_read)

    await asyncio.gather(
        m.spin_forever(),
        node.heartbeat(m, 0.5),
    )


if __name__ == "__main__":
    asyncio.run(main())
