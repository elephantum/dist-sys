01_echo:
	./maelstrom/maelstrom test -w echo --bin ./01_echo/app.py --node-count 1 --time-limit 10 --log-stderr

02_unique_id:
	./maelstrom/maelstrom test -w unique-ids --bin ./02_unique_id/app.py --node-count 3 --rate 1000 --time-limit 10 --availability total --nemesis partition

03_broadcast: 03a_broadcast 03b_broadcast 03c_broadcast

03a_broadcast:
	./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app.py --node-count 1 --time-limit 20 --rate 10

03b_broadcast:
	./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app.py --node-count 5 --time-limit 20 --rate 10

03c_broadcast:
	./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app.py --node-count 5 --time-limit 20 --rate 10 --nemesis partition

.PHONY: 01_echo 02_unique_id 03_broadcast 03a_broadcast 03b_broadcast 03c_broadcast