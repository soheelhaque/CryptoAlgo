#!/bin/bash -xe

# Update the box to the latest software
sudo yum update -y

# Set the timezone correctly for cron
sudo timedatectl set-timezone Europe/London

# Install QuestDB
cd ~
wget https://github.com/questdb/questdb/releases/download/6.4.3/questdb-6.4.3-rt-linux-amd64.tar.gz
tar xvf questdb-6.4.3-rt-linux-amd64.tar.gz

# Create QuestDB drive from snapshot
sudo ~/CryptoAlgo/Scripts/restore_questdb_snapshot.sh

# Setup systemd to run QuestDB
sudo cp ~/CryptoAlgo/Scripts/questdb.service /etc/systemd/system/questdb.service
sudo systemctl daemon-reload
sudo systemctl start questdb.service
sudo systemctl enable questdb.service

