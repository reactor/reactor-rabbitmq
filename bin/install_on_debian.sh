#!/bin/sh

sudo apt-get update -y
sudo apt-get install -y init-system-helpers socat adduser logrotate

cd /tmp/
wget https://dl.bintray.com/rabbitmq/rabbitmq-server-deb/rabbitmq-server_3.6.12-1_all.deb
sudo dpkg --install rabbitmq-server_3.6.12-1_all.deb
sudo rm rabbitmq-server_3.6.12-1_all.deb

sleep 3