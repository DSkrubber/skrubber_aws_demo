### Project info

This is a demo project to interact with different AWS services.

### Prerequisites

1) This project uses [Poetry](https://python-poetry.org/) for dependency and virtual environment management, you also need to install it.

2) You also need to create IAM user with programmatic access to EC2, S3, SNS, SQS, RDS, DynamoDB, Lambda and ElastiCache.

3) Also create and preconfigure EC2 instance, RDS database and redis ElastiCache cluster.

### Installation

1) Clone the repo:

`git clone https://git.epam.com/epm-uii/badgerdoc/back-end.git`

2) To install the required dependencies and set up a virtual environment run in the cloned directory:

`poetry install`

3) Add `.env` file with all requested variables to repo root.

### Run application

1) To run web application from repository root use command:

`poetry run uvicorn --host '0.0.0.0' --port 8000 app.main:app`

*Note: make sure to allow access to port 8000 in 'Inbound rules' of your EC2 instance.*

2) Since application runs you can interact with resources via SWAGGER:

`http://<your_EC2_instance_Public_IPv4_DNS_adress>/docs`