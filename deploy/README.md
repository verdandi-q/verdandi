# Deploy Verdandi

All three platforms need a cluster key. Run `verdandi keygen` to make one. Store the key as a secret. No template has a default key.

## Image

Build the image from the repository root:

    docker build -f deploy/Dockerfile -t verdandi:c5 .

Push the image to your registry. The node keeps its data in `/data`. It listens on port 8080 (HTTP) and port 7946 (peers).

## Kubernetes

1. Make the secret: `kubectl create secret generic verdandi-key --from-literal=key=<key>`
2. Install the chart:

       helm install vd deploy/helm --set clusterKey.secretName=verdandi-key --set image.repository=<registry>/verdandi

3. Optional: add `--set backupUrl=s3://bucket/prefix` to turn on the backup.

The chart makes a StatefulSet with 3 pods. The pods spread over zones. Use a cluster with 3 zones.

## ECS and Fargate

1. Store the key in AWS Secrets Manager.
2. Deploy `deploy/ecs/verdandi-ecs.yaml` with these parameters: `ImageUri`, `VpcId`, `Subnet1` to `Subnet3` (3 zones), `ClientCidr`, `ClusterKeySecretArn`.
3. Set `BackupUrl` on Fargate. A task replacement deletes the disk. Set `TaskRoleArn` to a role that can write to the bucket.
4. If you change `DesiredCount`, set `MinimumHealthyPercent` to one task less. Example: 66 for 3 tasks.

## EC2

1. Store the key as an SSM SecureString parameter.
2. Deploy `deploy/ec2/verdandi-ec2.yaml` with these parameters: `ImageUri`, `VpcId`, `Subnet1` to `Subnet3` (3 zones), `ClientCidr`, `ClusterKeyParameter`.
3. If you change `DesiredCapacity`, set `MinInServiceDuringUpdate` to one less.

The instance role cannot pull a private image or write to a backup bucket. Add those rights to the role if you need them.
