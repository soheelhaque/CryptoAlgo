#!/bin/bash

runuser -l ec2-user -c /home/ec2-user/restore_questdb_snapshot.sh
runuser -l ec2-user -c /home/ec2-user/install_cryptoalgo.sh