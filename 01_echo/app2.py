#!/bin/env python

import asyncio
from mstrm import Message, Mstrm


async def on_echo(m: Mstrm, msg: Message):
    assert msg.body["type"] == "echo"

    await m.reply(
        msg,
        {
            "type": "echo_ok",
            "echo": msg.body["echo"],
        },
    )


async def main():
    m = await Mstrm.create()
    m.on("echo", on_echo)

    await m.spin_forever()


if __name__ == "__main__":
    asyncio.run(main())
