#!/bin/bash
# Idempotent Lab 03 recovery cleanup. Deletes only IDs recorded in aws.env.
set -u

STATE=${1:-$PWD/aws.env}
if [ ! -f "$STATE" ]; then
  echo "No state file at $STATE; nothing deleted"
  exit 0
fi
source "$STATE"
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-ap-southeast-1}
set +e

if [ -n "${INSTANCE_IDS:-}" ]; then
  read -r -a IDS <<<"$INSTANCE_IDS"
  aws ec2 terminate-instances --instance-ids "${IDS[@]}" >/dev/null
  aws ec2 wait instance-terminated --instance-ids "${IDS[@]}"
fi
if [ -n "${KEY_NAME:-}" ]; then aws ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null; fi
if [ -n "${KEY_FILE:-}" ]; then rm -f "$KEY_FILE"; fi
if [ -n "${SG_ID:-}" ]; then
  for attempt in $(seq 1 30); do
    aws ec2 delete-security-group --group-id "$SG_ID" >/dev/null 2>&1 && break
    sleep 5
  done
fi
if [ -n "${ROUTE_ASSOC_ID:-}" ]; then
  aws ec2 disassociate-route-table --association-id "$ROUTE_ASSOC_ID" >/dev/null
fi
if [ -n "${ROUTE_TABLE_ID:-}" ]; then
  aws ec2 delete-route-table --route-table-id "$ROUTE_TABLE_ID" >/dev/null
fi
if [ -n "${IGW_ID:-}" ] && [ -n "${VPC_ID:-}" ]; then
  aws ec2 detach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" >/dev/null
  aws ec2 delete-internet-gateway --internet-gateway-id "$IGW_ID" >/dev/null
fi
if [ -n "${SUBNET_ID:-}" ]; then aws ec2 delete-subnet --subnet-id "$SUBNET_ID" >/dev/null; fi
if [ -n "${VPC_ID:-}" ]; then aws ec2 delete-vpc --vpc-id "$VPC_ID" >/dev/null; fi

echo -n 'Lab 03 active instances: '
aws ec2 describe-instances --filters Name=tag:Project,Values=ledgerline-ep04 Name=tag:Lab,Values=lab-03 \
  Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'length(Reservations[].Instances[])' --output text
echo -n 'Lab 03 VPCs: '
aws ec2 describe-vpcs --filters Name=tag:Project,Values=ledgerline-ep04 Name=tag:Lab,Values=lab-03 \
  --query 'length(Vpcs[])' --output text
