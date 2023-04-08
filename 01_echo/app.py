#!/bin/env python

import sys
from typing import Annotated, Generator, Literal, Union
from pydantic import BaseModel, Field


class Init(BaseModel):
    type: Literal["init"] = "init"

    msg_id: int
    node_id: str
    node_ids: list[str]


class InitOk(BaseModel):
    type: Literal["init_ok"] = "init_ok"

    in_reply_to: int


class Echo(BaseModel):
    type: Literal["echo"] = "echo"

    msg_id: int
    echo: str


class EchoOk(BaseModel):
    type: Literal["echo_ok"] = "echo_ok"

    msg_id: int
    in_reply_to: int
    echo: str


Body = Annotated[Union[Init, InitOk, Echo, EchoOk], Field(discriminator="type")]


class Message(BaseModel):
    src: str
    dest: str
    body: Body


def handler() -> Generator[Message, Message, None]:
    sys.stderr.write("Starting echo handler\n")

    init_message: Message = yield
    assert isinstance(init_message.body, Init)

    msg = yield Message(
        src=init_message.dest,
        dest=init_message.src,
        body=InitOk(in_reply_to=init_message.body.msg_id),
    )

    while True:
        msg = yield Message(
            src=msg.dest,
            dest=msg.src,
            body=EchoOk(
                msg_id=msg.body.msg_id,
                in_reply_to=msg.body.msg_id,
                echo=msg.body.echo,
            ),
        )


def main():
    sys.stderr.write("Starting echo app\n")

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
