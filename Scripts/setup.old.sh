#!/bin/bash -xe
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Update the box to the latest software
sudo yum update -y

# Set the timezone correctly for cron
sudo timedatectl set-timezone Europe/London

# Install git
sudo yum install git -y

# Install CryptoAlgo
cd ~
git clone https://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@github.com/soheelhaque/CryptoAlgo.git

# Install requirements for CryptoAlgo
sudo yum install -y gcc postgresql-devel
sudo yum install -y python3-devel
sudo python3 -m pip install -r ~/CryptoAlgo/requirements.txt

# Make all the scripts executable
sudo chmod +x ~/CryptoAlgo/Scripts/*

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

# Add crontab entry to run CryptoAlgo daily at 10am
(crontab -l 2>/dev/null; echo "0 10 * * * cd /home/ec2-user/CryptoAlgo && /usr/bin/python3 CryptoPriceDBGateway.py > /dev/null") | crontab -
