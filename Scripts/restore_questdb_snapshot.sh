#!/bin/bash -xe
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Instructions
# Add this script to ~/CryptoAlgo/scripts and setup a crontab entry to run this script at reboot
# Then create an AMI

# Set up parameters
mount_point=/var/lib/questdb
volume_type=gp2
size=8
device=/dev/xvdf

# Get instance details
instance_id=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
region=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/region`
availability_zone=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`

# Get latest snapshot
snapshot_id=`aws ec2 describe-snapshots --region $region --filters Name=tag-key,Values=QuestDB --query="max_by(Snapshots, &StartTime).SnapshotId" --output text`

# Create volume from snapshot and attach to this instance
volume_id=`aws ec2 create-volume --volume-type $volume_type --size $size --availability-zone $availability_zone --region $region --snapshot-id $snapshot_id --tag-specifications 'ResourceType=volume,Tags=[{Key=QuestDB,Value=True}]' --query "VolumeId" --output text`

# Wait for creation to finish
status=`aws ec2 describe-volumes --volume-ids $volume_id  --region $region --query "Volumes[*].State"`
while [[ "$status" != *"available"* ]]
do
	sleep 5
	status=`aws ec2 describe-volumes --volume-ids $volume_id  --region $region --query "Volumes[*].State"`
done

aws ec2 attach-volume --volume-id $volume_id --instance-id $instance_id --device $device --region $region

# Wait for attachment to finish
status=`aws ec2 describe-volumes --volume-ids $volume_id  --region $region --query "Volumes[*].Attachments[*].State"`
while [[ "$status" != *"attached"* ]]
do
	sleep 5
	status=`aws ec2 describe-volumes --volume-ids $volume_id --region $region --query "Volumes[*].Attachments[*].State"`
done

# Modify the device so that it is deleted when the instance is terminated
aws ec2 modify-instance-attribute --instance-id $instance_id --region $region --block-device-mappings "[{\"DeviceName\": \"$device\",\"Ebs\":{\"DeleteOnTermination\":true}}]"

# Mount the drive
sudo mkdir -p $mount_point
sudo mount $device $mount_point

# Return status
if [[ `lsblk -o MOUNTPOINT -nr $device` == *"$mount_point"* ]]
then
	echo
	echo '********** SUCCESS! **********'
	echo
	exit 0
fi

echo
echo '********** FAILED! **********'
echo
exit 1