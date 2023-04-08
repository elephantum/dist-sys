Python solutions for https://fly.io/dist-sys/

# 01 echo

    ./maelstrom/maelstrom test -w echo --bin ./01_echo/app.py --node-count 1 --time-limit 10 --log-stderr

# 02 unique id

    ./maelstrom/maelstrom test -w unique-ids --bin ./02_unique_id/app.py --node-count 3 --rate 1000 --time-limit 10 --availability total --nemesis partition
