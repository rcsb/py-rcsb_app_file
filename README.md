# py-rcsb_app_file

File Access Service Application

### Installation

Download the library source software from the project repository:

```bash

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_file_chem.git

```

Optionally, run test suite (Python 3.9) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).

```bash
pip install rcsb.app.file

or from the local repository directory:

pip install .
```

# Deployment on Local Server

### Build Docker Container

```bash

In directory that contains “Dockerfile.devel”

docker build -t fileapp -f Dockerfile.devel .

```

### Run docker container

```bash

docker run –rm –name fileapp -p 80:8000 fileapp

```

-d runs container in the background, allowing user to 

–rm removes the container after it is stopped

–name allows user to choose a name for the container

-p allows user to choose a port, 80:8000 is used in this case, as the port 8000 is exposed in the current dockerfile


# Deployment on AWS Server

Start in the directory that contains the dockerfile (dockerfile.devel in this case)
```bash

docker build -t fileapp -f Dockerfile.devel .

```

Launch an EC2 instance using an AWS account

Create or select key pair

Run “chmod 400 fileapp.pem” in a terminal and replace fileapp.pem with the name of your key

Go to the directory where the .pem key is located

In the browser where you launched the ec2 instance, click on the running instance, and then click connect to find the command to ssh into the instance

```bash

sudo yum update -y
amazon-linux-extras install docker
sudo service docker start
sudo usermod -a -G docker ec2-user

```

Now go to the AWS console in a browser
Go to the Identity and Access Management (IAM) Management Console

Create a user named Administrator
For the group select Administrator

When done creating the user, save the access key and secret access key
Note the region for your EC2 server
Choose a format for output (e.g. “json”)

Go back to local terminal and use the command “aws configure”
Enter the access key, secret access key, region name, and output format

Now ssh into the EC2 server and repeat the same steps with “aws configure”

Once aws configure details are matching on local machine and the EC2 instance,
Create an ECR from the AWS console. 

Select the repository you created, and then click view push commands

Follow the instructions under push commands to tag and push the docker image to the server

Copy the Image URI from the image you pushed to the repository

After ssh into the EC2 instance, use the

```bash
docker pull <image URI>
```

Return to the EC2 dashboard, click on the running instance, go to the security tab
Use the command

```
docker run -p 80:8000 <image name>
```

-p 80:8000 map port 80 of the instance to port 8000 in docker, and start running the container. 

Adding the -d flag will run the container in the background, so you can continue to use the terminal

Click edit inbound rules

Add a new rule, and select all traffic for type*(this may have to be changed)
Note the public ipv4 address from the running EC2 instance

In a browser type “http://ipv4address:port” to check if container is running properly
