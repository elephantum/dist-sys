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

    original: bool = True


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


def handler() -> Generator[list[Message], Message, None]:
    sys.stderr.write("Starting echo handler\n")

    init_message: Message = yield
    assert isinstance(init_message.body, Init)

    node_id = init_message.body.node_id
    node_ids = init_message.body.node_ids

    msg = yield [
        Message(
            src=init_message.dest,
            dest=init_message.src,
            body=InitOk(in_reply_to=init_message.body.msg_id),
        )
    ]

    known_messages: Set[int] = set()

    while True:
        res: list[Message] = []

        match msg.body:
            case Topology(msg_id=msg_id):
                res.append(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=TopologyOk(in_reply_to=msg_id),
                    )
                )
            case Broadcast(msg_id=msg_id, message=message, original=original):
                known_messages.add(message)
                res.append(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=BroadcastOk(in_reply_to=msg_id),
                    )
                )

                if original:
                    for node in node_ids:
                        if node != node_id:
                            res.append(
                                Message(
                                    src=node_id,
                                    dest=node,
                                    body=Broadcast(
                                        msg_id=msg_id, message=message, original=False
                                    ),
                                )
                            )
            case Read(msg_id=msg_id):
                res.append(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=ReadOk(in_reply_to=msg_id, messages=list(known_messages)),
                    )
                )

        msg = yield res


def main():
    sys.stderr.write("Starting app\n")

    h = handler()

    h.send(None)

    for line in sys.stdin:
        sys.stderr.write(f"Received: {line}")

        msg = Message.parse_raw(line)

        out_msgs = h.send(msg)

        for out_msg in out_msgs:
            sys.stderr.write(f"Sending: {out_msg.json()}\n")

            sys.stdout.write(f"{out_msg.json()}\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
