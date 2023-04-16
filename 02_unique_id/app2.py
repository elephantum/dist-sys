#!/bin/env python

import asyncio
from mstrm import Message, Mstrm


class Node:
    def __init__(self) -> None:
        self.inc = 0

    async def on_generate(self, m: Mstrm, msg: Message) -> None:
        await m.reply(
            msg,
            {
                "type": "generate_ok",
                "id": f"{m.node_id}-{self.inc}",
            },
        )

        self.inc += 1


async def main():
    m = await Mstrm.create()

    node = Node()

    m.on("generate", node.on_generate)

    await m.spin_forever()


if __name__ == "__main__":
    asyncio.run(main())
