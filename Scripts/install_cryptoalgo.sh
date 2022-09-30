#!/bin/bash -xe
exec > >(tee -i ~/install_cryptoalgo.log)
exec 2>&1

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
rm -rf CryptoAlgo
git clone https://github.com/soheelhaque/CryptoAlgo.git CryptoAlgo

# Install requirements for CryptoAlgo
sudo yum install -y gcc postgresql-devel
sudo yum install -y python3-devel
sudo python3 -m pip install -r ~/CryptoAlgo/requirements.txt

# Make all the scripts executable
sudo chmod +x ~/CryptoAlgo/Scripts/*

# Add crontab entry to run CryptoAlgo daily at 10am
crontab -l | cat - ~/CryptoAlgo/Scripts/cryptoalgo.crontab | crontab -
