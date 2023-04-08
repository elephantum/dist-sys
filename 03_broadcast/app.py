#!/bin/env python

from dataclasses import dataclass
import select
import sys
import time
from typing import Annotated, Callable, Dict, Generator, Literal, Set, Union
from pydantic import BaseModel, Field


class InReplyTo(BaseModel):
    in_reply_to: int


class Init(BaseModel):
    type: Literal["init"] = "init"

    msg_id: int
    node_id: str
    node_ids: list[str]


class InitOk(InReplyTo):
    type: Literal["init_ok"] = "init_ok"


class Broadcast(BaseModel):
    type: Literal["broadcast"] = "broadcast"

    msg_id: int
    message: int

    original: bool = True


class BroadcastOk(InReplyTo):
    type: Literal["broadcast_ok"] = "broadcast_ok"


class Read(BaseModel):
    type: Literal["read"] = "read"

    msg_id: int


class ReadOk(InReplyTo):
    type: Literal["read_ok"] = "read_ok"
    messages: list[int]


class Topology(BaseModel):
    type: Literal["topology"] = "topology"

    msg_id: int
    topology: Dict[str, list[str]]


class TopologyOk(InReplyTo):
    type: Literal["topology_ok"] = "topology_ok"


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


@dataclass
class PendingReply:
    msg: Message
    handler: Callable[[Message], None]
    sent_at: float


class Node:
    def __init__(self, node_id: str, node_ids: list[str]):
        self.node_id = node_id
        self.node_ids = node_ids

        self.known_messages: Set[int] = set()

        # Message ID -> Callback
        self.pending_replies: Dict[int, PendingReply] = {}

        # infinite generator of incrementally increasing message IDs
        self.message_id_gen = (i for i in range(1, sys.maxsize))

    def heartbeat(self) -> None:
        # check for pending replies that have timed out
        for msg_id, pending_reply in self.pending_replies.items():
            if time.time() - pending_reply.sent_at > 1:
                sys.stderr.write(f"Reply timed out: {pending_reply.msg.json()}\n")

                # resend
                sys.stderr.write(f"Resending: {pending_reply.msg.json()}\n")
                self.send(pending_reply.msg, pending_reply.handler)

    def send(
        self,
        msg: Message,
        handler: Callable[[Message], None] | None = None,
    ) -> None:
        sys.stderr.write(f"Sending: {msg.json()}\n")

        sys.stdout.write(f"{msg.json()}\n")
        sys.stdout.flush()

        if handler is not None:
            self.pending_replies[msg.body.msg_id] = PendingReply(
                msg=msg,
                handler=handler,
                sent_at=time.time(),
            )

    def handle(self, msg: Message) -> None:
        match msg:
            case InReplyTo():
                if msg.body.in_reply_to in self.pending_replies:
                    self.pending_replies[msg.body.in_reply_to].handler(msg)
                    del self.pending_replies[msg.body.in_reply_to]
                else:
                    sys.stderr.write(
                        f"Received reply to unknown message: {msg.json()}\n"
                    )

            case _:
                self.handle_request(msg)

    def handle_request(self, msg: Message) -> None:
        # pending replies stats
        sys.stderr.write(f"Pending replies: {len(self.pending_replies)}\n")

        match msg.body:
            case Topology(msg_id=msg_id):
                self.send(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=TopologyOk(in_reply_to=msg_id),
                    )
                )
            case Broadcast(msg_id=msg_id, message=message, original=original):
                self.known_messages.add(message)
                self.send(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=BroadcastOk(in_reply_to=msg_id),
                    )
                )

                if original:
                    for node in self.node_ids:
                        if node != self.node_id:
                            self.send(
                                Message(
                                    src=self.node_id,
                                    dest=node,
                                    body=Broadcast(
                                        msg_id=next(self.message_id_gen),
                                        message=message,
                                        original=False,
                                    ),
                                ),
                                lambda msg: None,
                            )
            case Read(msg_id=msg_id):
                self.send(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=ReadOk(
                            in_reply_to=msg_id, messages=list(self.known_messages)
                        ),
                    )
                )


def read_message_or_timeout(
    timeout: float,
) -> Message | None:
    """Read a line from stdin with a timeout."""
    start = time.time()
    while True:
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            line = sys.stdin.readline()
            if line:
                return Message.parse_raw(line)
        if time.time() - start > timeout:
            return None

        time.sleep(0.01)


def main():
    sys.stderr.write("Starting app\n")

    # receive init message
    init_message = read_message_or_timeout(1)
    assert init_message is not None
    sys.stderr.write(f"Received: {init_message.json()}\n")
    assert isinstance(init_message.body, Init)

    # send message with init_ok body
    res = Message(
        src=init_message.dest,
        dest=init_message.src,
        body=InitOk(in_reply_to=init_message.body.msg_id),
    )
    sys.stderr.write(f"Sending: {res.json()}\n")
    sys.stdout.write(f"{res.json()}\n")
    sys.stdout.flush()

    # init node
    node = Node(init_message.body.node_id, init_message.body.node_ids)

    # loop over messages, process them and send responses
    while True:
        msg = read_message_or_timeout(1)
        if msg is not None:
            sys.stderr.write(f"Received: {msg.json()}\n")
            node.handle(msg)
        else:
            node.heartbeat()


if __name__ == "__main__":
    main()
