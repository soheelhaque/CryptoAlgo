#### install from GIT
No Key is required for CryptoAlgo - it is open.
From the user base directory:

    git clone https://ls a-lgithub.com/SohellHaque/CryptoAlgo.git

#### update code:
Log into the running server using ec2-user account.
Move into the CryptoAlgo directory

    cd CryptoAlgo

Once in the correct directory, do a git pull using a valid git username/token:

    git pull https://github.com/SoheelHaque/CryptoAlgo.git

#### create & activate venv
Do this if venv does not already exist (ie new install)

    cd CryptoAlgo/
    python3 -m venv venv

#### install dependencies (or any new dependencies if upgrading)

    source venv/bin/activate
    pip install -r requirements.txt

## Check database server works
If the database is already running under systemd, then it needs to be stopped and restarted.

#### run up server (if no systemd set up)
To start up the API server execute:

    cd ~
    .questdb-7.2-rt-linux-amd64/bin/questdb.sh start -d /var/lib/questdb

You can connect to it and check it works.

## Create systemd entry

#### Create a service file
The service file does contain the 'username' in the directory; so it may need to be modified dependign on the user used to setup the isntallation

    [Unit]
    Description=QuestDB
    Documentation=https://www.questdb.io/docs/introduction
    After=network.target
    RequiresMountsFor=/var/lib/questdb
    
    [Service]
    Type=forking
    Restart=always
    RestartSec=2
    User=ec2-user
    ExecStart=/home/ec2-user/questdb-7.2-rt-linux-amd64/bin/questdb.sh start -d /var/lib/questdb

    [Install]
    WantedBy=multi-user.target

#### Set up systemd

    sudo systemctl daemon-reload
    sudo systemctl enable questdb.service
    sudo systemctl start questdb.service

#### check status
    sudo systemctl status questdb.service

### Run upgrade script.
You will need to start up the venv to be able to run the database migration code.

    cd CryptoAlgo/
    source venv/bin/activate
    python3 -m MigrateDatabase.py