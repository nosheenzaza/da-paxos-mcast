#!/usr/bin/env bash

LOSS=$1

if [[ x$LOSS == "x" ]]; then
	echo "Usage: $0 <loss percentage (0.0 to 1.0)>"
    exit 1
fi


# sudo iptables -A INPUT -d 239.0.0.1\
#     -m statistic --mode random --probability $LOSS -j DROP

# Create pipes
sudo  ipfw add pipe 1 ip from any to 239.0.0.1
sudo  ipfw add pipe 2 ip from 239.0.0.1 to any


# Configure the pipes we just created:
# sudo ipfw pipe 1 config delay 250ms bw 1Mbit/s plr 0.1
# sudo ipfw pipe 2 config delay 250ms bw 1Mbit/s plr 0.1

sudo ipfw pipe 1 config plr $LOSS
sudo ipfw pipe 2 config plr $LOSS
