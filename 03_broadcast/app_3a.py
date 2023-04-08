#!/bin/env python

import sys
from typing import Annotated, Dict, Generator, Literal, Set, Union
from pydantic import BaseModel, Field


class Init(BaseModel):
    type: Literal["init"] = "init"

    msg_id: int
    node_id: str
    node_ids: list[str]


class InitOk(BaseModel):
    type: Literal["init_ok"] = "init_ok"

    in_reply_to: int


class Broadcast(BaseModel):
    type: Literal["broadcast"] = "broadcast"

    msg_id: int
    message: int


class BroadcastOk(BaseModel):
    type: Literal["broadcast_ok"] = "broadcast_ok"

    in_reply_to: int


class Read(BaseModel):
    type: Literal["read"] = "read"

    msg_id: int


class ReadOk(BaseModel):
    type: Literal["read_ok"] = "read_ok"

    in_reply_to: int
    messages: list[int]


class Topology(BaseModel):
    type: Literal["topology"] = "topology"

    msg_id: int
    topology: Dict[str, list[str]]


class TopologyOk(BaseModel):
    type: Literal["topology_ok"] = "topology_ok"

    in_reply_to: int


Body = Annotated[
    Union[
        Init,
        InitOk,
        Broadcast,
        BroadcastOk,
        Read,
        ReadOk,
        Topology,
        TopologyOk,
    ],
    Field(discriminator="type"),
]


class Message(BaseModel):
    src: str
    dest: str
    body: Body


def handler() -> Generator[Message, Message, None]:
    sys.stderr.write("Starting echo handler\n")

    init_message: Message = yield
    assert isinstance(init_message.body, Init)

    node_id = init_message.body.node_id

    msg = yield Message(
        src=init_message.dest,
        dest=init_message.src,
        body=InitOk(in_reply_to=init_message.body.msg_id),
    )

    known_messages: Set[int] = set()

    while True:
        match msg.body:
            case Broadcast(msg_id=msg_id, message=message):
                known_messages.add(message)
                res = Message(
                    src=msg.dest,
                    dest=msg.src,
                    body=BroadcastOk(in_reply_to=msg_id),
                )
            case Read(msg_id=msg_id):
                res = Message(
                    src=msg.dest,
                    dest=msg.src,
                    body=ReadOk(in_reply_to=msg_id, messages=list(known_messages)),
                )
            case Topology(msg_id=msg_id):
                res = Message(
                    src=msg.dest,
                    dest=msg.src,
                    body=TopologyOk(in_reply_to=msg_id),
                )

        msg = yield res


def main():
    sys.stderr.write("Starting app\n")

    h = handler()

    h.send(None)

    for line in sys.stdin:
        sys.stderr.write(f"Received: {line}")

        msg = Message.parse_raw(line)

        res = h.send(msg)

        sys.stderr.write(f"Sending: {res.json()}\n")

        sys.stdout.write(f"{res.json()}\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
