import asyncio
from asyncio import Future
import sys
from typing import Any, Callable, Dict

from pydantic import BaseModel


class Message(BaseModel):
    src: str
    dest: str
    body: Dict[str, Any]


async def connect_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer


async def read_messages(reader):
    while True:
        line = await reader.readline()
        if not line:
            break
        env = Message.parse_raw(line)
        sys.stderr.write(f"Received: {env.json()}\n")

        yield env


class Mstrm:
    def __init__(
        self,
        reader,
        writer,
        node_id: str,
        node_ids: list[str],
    ):
        self.reader = reader
        self.writer = writer

        self.node_id = node_id
        self.node_ids = node_ids

        self.message_id_gen = (i for i in range(1, sys.maxsize))

        self.callbacks: Dict[str, Callable] = {}
        self.call_futures: Dict[str, asyncio.Future] = {}

    @classmethod
    async def create(cls) -> "Mstrm":
        reader, writer = await connect_stdin_stdout()

        messages_gen = read_messages(reader)

        init_msg = await messages_gen.__anext__()
        assert init_msg.body["type"] == "init"

        res = cls(
            reader=reader,
            writer=writer,
            node_id=init_msg.body["node_id"],
            node_ids=[
                i for i in init_msg.body["node_ids"] if i != init_msg.body["node_id"]
            ],
        )

        await res.reply(init_msg, {"type": "init_ok"})

        topology_msg = await messages_gen.__anext__()
        assert topology_msg.body["type"] == "topology"

        await res.reply(topology_msg, {"type": "topology_ok"})

        return res

    def on(self, event_type: str, callback: Callable) -> None:
        self.callbacks[event_type] = callback

    async def call(self, dest: str, message: Dict) -> Future:
        message["msg_id"] = next(self.message_id_gen)

        await self.send(dest, message)

        future = asyncio.get_running_loop().create_future()
        self.call_futures[message["msg_id"]] = future

        return future

    async def reply(self, original_message: Message, message: Dict) -> None:
        message["msg_id"] = next(self.message_id_gen)
        message["in_reply_to"] = original_message.body["msg_id"]

        await self.send(original_message.src, message)

    async def send(self, dest: str, message: Dict) -> None:
        env = Message(src=self.node_id, dest=dest, body=message)

        self.writer.write(env.json().encode("utf-8") + b"\n")
        await self.writer.drain()

    async def spin_forever(self):
        async for message in read_messages(self.reader):
            if "in_reply_to" in message.body:
                if message.body["in_reply_to"] in self.call_futures:
                    self.call_futures[message.body["in_reply_to"]].set_result(
                        message.body
                    )

                    del self.call_futures[message.body["in_reply_to"]]

            if message.body["type"] in self.callbacks:
                await self.callbacks[message.body["type"]](self, message)


async def main():
    m = await Mstrm.create()

    await m.spin_forever()


if __name__ == "__main__":
    asyncio.run(main())
