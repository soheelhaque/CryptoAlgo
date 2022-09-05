#!/bin/bash -xe
exec > >(tee /var/log/cryptoalgo-install.log|logger -t user-data -s 2>/dev/console) 2>&1

# Instructions
# Add this script to ~/CryptoAlgo/scripts and setup a crontab entry to run this script at reboot
# Then create an AMI

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
