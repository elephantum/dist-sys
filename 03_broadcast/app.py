#!/bin/env python

from dataclasses import dataclass
import select
import sys
import time
from typing import Annotated, Callable, Dict, Generator, Iterator, Literal, Set, Union
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


class BroadcastOk(InReplyTo):
    type: Literal["broadcast_ok"] = "broadcast_ok"


class BatchBroadcast(BaseModel):
    type: Literal["batch_broadcast"] = "batch_broadcast"

    msg_id: int
    messages: list[int]


class BatchBroadcastOk(InReplyTo):
    type: Literal["batch_broadcast_ok"] = "batch_broadcast_ok"


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
        BatchBroadcast,
        BatchBroadcastOk,
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


class Heartbeat(BaseModel):
    pass


@dataclass
class PendingReply:
    msg: Message
    handler: Callable[[Message], None]
    sent_at: float


class Node:
    def __init__(self, node_id: str, node_ids: list[str]):
        self.node_id = node_id
        self.node_ids = set(node_ids) - {node_id}

        self.known_messages: Set[int] = set()

        # Message ID -> Callback
        self.pending_replies: Dict[int, PendingReply] = {}

        # infinite generator of incrementally increasing message IDs
        self.message_id_gen = (i for i in range(1, sys.maxsize))

        # Node ID -> Set of pending messages (ints)
        self.pending_broadcasts: Dict[str, Set[int]] = {
            node: set() for node in self.node_ids
        }

    def heartbeat(self) -> None:
        sys.stderr.write("Handle heartbeat\n")
        # send batch broadcasts of pending messages
        for node, messages in self.pending_broadcasts.items():
            if messages:
                self.send(
                    Message(
                        src=self.node_id,
                        dest=node,
                        body=BatchBroadcast(
                            msg_id=next(self.message_id_gen),
                            messages=list(messages),
                        ),
                    ),
                    # clear messages that were sent on success
                    lambda msg: self.pending_broadcasts[msg.dest].difference_update(
                        msg.body.messages
                    ),
                )

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
            case Broadcast(msg_id=msg_id, message=message):
                self.known_messages.add(message)

                # add message to pending broadcasts
                for node in self.node_ids:
                    self.pending_broadcasts[node].add(message)

                self.send(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=BroadcastOk(in_reply_to=msg_id),
                    )
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
            case BatchBroadcast(msg_id=msg_id, messages=messages):
                self.known_messages.update(messages)
                self.send(
                    Message(
                        src=msg.dest,
                        dest=msg.src,
                        body=BatchBroadcastOk(in_reply_to=msg_id),
                    )
                )


def read_message_with_heartbeat(
    timeout: float,
) -> Iterator[Message | Heartbeat]:
    """Read a line from stdin with a timeout."""
    start = time.time()
    while True:
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            line = sys.stdin.readline()
            if line:
                msg = Message.parse_raw(line)
                sys.stderr.write(f"Received: {msg.json()}\n")
                yield msg
        if time.time() - start > timeout:
            yield Heartbeat()
            start = time.time()

        time.sleep(0.01)


def main():
    sys.stderr.write("Starting app\n")

    line = sys.stdin.readline()
    init_message = Message.parse_raw(line)
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

    input = read_message_with_heartbeat(0.5)

    # init node
    node = Node(init_message.body.node_id, init_message.body.node_ids)

    # loop over messages, process them and send responses
    while True:
        msg = next(input)
        match msg:
            case Heartbeat():
                node.heartbeat()
            case Message():
                node.handle(msg)


if __name__ == "__main__":
    main()
