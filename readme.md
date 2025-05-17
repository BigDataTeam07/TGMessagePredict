### Prerequisites (cluster setup, dependencies)

- Python 3.12.X
- aiohttp
- aiohttp
- boto3
- google-auth
- google-cloud-translate

### Build & run commands

1. Be sure you have a Google Cloud project with the Translate API enabled.
2. Set up your Google Cloud credentials. Download the JSON key file for your service account and set the `KEY_PATH`
   variable to point to it.
    - Sample: `KEY_PATH = "abiding-root-413802-************.json"`
3. Set CLUSTER_ARN, SECRET_NAME, WATCH_CHAT_IDS, PREDICT_URL in `handler.py`
4. Start a t4g.nano instance:(if you don't want this service on cloud, skip this step)
    1. Generate a key pair in AWS EC2.
       ```
       aws ec2 create-key-pair --key-name kafka-stream-predict-ec2-key --query 'KeyMaterial' --output text > my-ec2-key.pem
       ```
    2. Create Security Group:
       ```
       aws ec2 create-security-group --group-name <your-security-group-id> --description "Allow SSH" --vpc-id <your-vpc-id>
       ```
       ```
       aws ec2 authorize-security-group-ingress --group-name <your-security-group-id> --protocol tcp --port 22 --cidr 0.0.0.0/0
       ```
    3. Get the AMI ID for the t4g.nano instance:
       ```
       aws ssm get-parameters --names /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --region ap-southeast-1 --query 'Parameters[0].Value' --output text
       ```
    4. Launch the t4g.nano instance:
       ```
         aws ec2 run-instances --image-id <ami-id> --instance-type t4g.nano --key-name kafka-stream-predict-ec2-key --security-group-ids <your-security-group-id> --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=UserSentimentPredict-Ubuntu2404}]' --region ap-southeast-1
       ```
    5. SSH into the instance:
       ```
       ssh -i my-ec2-key.pem ubuntu@<your-instance-ip>
       ```
    6. update the instance and set up the environment:
       ```
       sudo apt update
       sudo apt install -y python3.12-venv
       python3 -m venv venv
       source venv/bin/activate
       pip install --upgrade pip
       pip install aiokafka aiohttp boto3 google-cloud-translate google-auth
       ```
    7. Upload the code and Google Cloud Credentials to the instance:
       ```
       scp -i TGPredict-key.pem handler.py abiding-root-413802-************.json ubuntu@<PublicDNS>:/home/ubuntu/
       ```
    8. Create Service File `/etc/systemd/system/kafka-stream-predict.service`:
       ```
       sudo tee /etc/systemd/system/musicbot.service << 'EOF'
       [Unit]
       Description=Kafka Stream Predict Service
       After=network.target

       [Service]
       Type=simple
       User=ubuntu
       WorkingDirectory=/home/ubuntu
       ExecStart=/home/ubuntu/venv/bin/python /home/ubuntu/handler.py
       Restart=always
       RestartSec=5
       StandardOutput=journal
       StandardError=journal

       [Install]
       WantedBy=multi-user.target
       EOF
       ```
    9. Start the service:
       ```
       sudo systemctl daemon-reload
       sudo systemctl enable kafka-stream-predict
       sudo systemctl start kafka-stream-predict
       ```
       This service will now process messages from the Kafka topic and send predictions to the specified URL.
    10. If you encounter `botocore.exceptions.NoCredentialsError: Unable to locate credentials`
        - Create an IAM role with policy `AmazonMSKReadOnlyAccess`, `SecretsManagerReadWrite`, `AllowSecretsKMSDecrypt`
          and attach it to the instance.

### Directory structure

None

### Contact

e1285202@u.nus.edu