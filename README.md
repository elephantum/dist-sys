Python solutions for https://fly.io/dist-sys/

# 01 echo

```shell
./maelstrom/maelstrom test -w echo --bin ./01_echo/app.py --node-count 1 --time-limit 10 --log-stderr
```

# 02 unique id

``` shell
./maelstrom/maelstrom test -w unique-ids --bin ./02_unique_id/app.py --node-count 3 --rate 1000 --time-limit 10 --availability total --nemesis partition
```

# 03 broadcast

```shell
# 3a
./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app.py --node-count 1 --time-limit 20 --rate 10

# 3b
./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app.py --node-count 5 --time-limit 20 --rate 10
```