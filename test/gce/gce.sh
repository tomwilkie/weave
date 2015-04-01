#!/bin/bash

set -ex

# Poor mans vagrant; cannot run vagrant on travis!
DIR=$(dirname $0)
ACCOUNT=710385362057-ceu81pfi010sree1ld07aravu09p1gko@developer.gserviceaccount.com
KEY_FILE=$DIR/gce_private_key.json
SSH_KEY_FILE=$DIR/gce_ssh_key
PROJECT=positive-cocoa-90213
IMAGE=ubuntu-14-04
ZONE=us-central1-a
NUM_HOSTS=2

# Setup authentication
gcloud auth activate-service-account $ACCOUNT --key-file $KEY_FILE
gcloud config set project $PROJECT

# Delete all vms in this account
function destroy {
	names="$(gcloud compute instances list --format=yaml | grep "^name\:" | cut -d: -f2 | xargs echo)"
	if [ -n "$names" ]; then
		gcloud compute instances delete --zone $ZONE -q $names
	fi
}

# Create new set of VMS
function setup {
	destroy

	# Create and setup some VMs
	for i in $(seq 1 $NUM_HOSTS); do
		name="host$i"
		gcloud compute instances create $name --image $IMAGE --zone $ZONE
	done

	gcloud compute config-ssh --ssh-key-file $SSH_KEY_FILE

	hosts=
	for i in $(seq 1 $NUM_HOSTS); do
		name="host$i.$ZONE.$PROJECT"
		hosts="$hosts $name"
		ssh -t $name sudo bash -x -s <<EOF
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9;
echo deb https://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list;
apt-get update -qq;
apt-get install -q -y --force-yes --no-install-recommends lxc-docker ethtool;
usermod -a -G docker vagrant;
echo 'DOCKER_OPTS="-H unix:///var/run/docker.sock -H tcp://0.0.0.0:2375"' >> /etc/default/docker;
service docker restart
EOF
	done

	export HOSTS="$hosts"
}

case "$1" in
setup)
	setup
	;;

destroy)
	destroy
	;;
esac
