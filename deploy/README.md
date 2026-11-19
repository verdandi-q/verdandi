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

The chart makes a StatefulSet with 3 pods. The pods spread over zones.

The value `zoneSpread.whenUnsatisfiable` is `ScheduleAnyway` by default. With fewer than 3 zones, the pods still start, but two pods can share a zone.
Set `--set zoneSpread.whenUnsatisfiable=DoNotSchedule` only when the cluster has at least 3 zones. Then no zone holds two pods. With fewer zones, the extra pods stay pending.

## ECS and Fargate

1. Store the key in AWS Secrets Manager.
2. Deploy `deploy/ecs/verdandi-ecs.yaml` with these parameters: `ImageUri`, `VpcId`, `Subnet1` to `Subnet3` (3 zones), `ClientCidr`, `ClusterKeySecretArn`.
3. Set `BackupUrl` on Fargate. A task replacement deletes the disk. Set `TaskRoleArn` to a role that can write to the bucket.
4. The template sets `MinimumHealthyPercent` from `DesiredCount` (1 to 10), so a deploy stops one task at a time. CloudFormation cannot do arithmetic, so a table in the template holds the values. For more than 10 tasks, extend the `Sizing` table.
5. Set `CertificateArn` to an ACM certificate to use HTTPS. See "HTTPS" below.

## EC2

1. Store the key as an SSM SecureString parameter.
2. Deploy `deploy/ec2/verdandi-ec2.yaml` with these parameters: `ImageUri`, `VpcId`, `Subnet1` to `Subnet3` (3 zones), `ClientCidr`, `ClusterKeyParameter`.
3. The template sets the rolling update size from `DesiredCapacity` (1 to 10), so an update stops one instance at a time. A table in the template holds the values. For more than 10 instances, extend the `Sizing` table.
4. Set `EcrRepositoryArn` if `ImageUri` is in a private ECR repository. The role then gets pull rights for that repository only.
5. Set `BackupUrl` to give the role read and write rights to that bucket only.
6. Set `CertificateArn` to an ACM certificate to use HTTPS. See "HTTPS" below.

On scale-in, the lifecycle hook `verdandi-stop` holds the instance. A small service on the instance stops the container (up to 120 s), then completes the hook. The role can complete the hook of this group only.

## HTTPS

By default the load balancer listens on plain HTTP, port 80. Plain HTTP sends the cluster-key bearer token of the `/n/` routes in clear. Use plain HTTP only on a trusted network.

Set `CertificateArn` to listen on HTTPS, port 443. Clients then use port 443 only. Use a DNS name that the certificate covers, not the load balancer name.
The nodes still reach the load balancer on HTTP, port 80, because they have no TLS client. The security group allows port 80 from the nodes only.
