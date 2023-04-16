01_echo:
	./maelstrom/maelstrom test -w echo --bin ./01_echo/app2.py --node-count 1 --time-limit 10 --log-stderr

02_unique_id:
	./maelstrom/maelstrom test -w unique-ids --bin ./02_unique_id/app2.py --node-count 3 --rate 1000 --time-limit 10 --availability total --nemesis partition

03_broadcast:
	./maelstrom/maelstrom test -w broadcast --bin ./03_broadcast/app2.py --node-count 25 --time-limit 20 --rate 100 --latency 100 # --nemesis partition 

.PHONY: 01_echo 02_unique_id 03_broadcast