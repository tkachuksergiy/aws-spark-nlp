# Apache Spark on AWS EC2 use case for NLP exercise: Amazon comments mining

My master's thesis was the inspiration for the project. The topic of which is “The use of natural language processing for predictive purposes in socio-economic applications”. In order to write the paper I was to learn couple of widely used frameworks and sharpen my Python programming skill. I went through miscellaneous problems ranged from environmental set-up to PySpark scrips debugging, hence I believe that sharing my experience and gained knowledge could help anyone who struggles with the same issue. Full text of the thesis (in Polish) is located in this repository.    
![python](https://www.101computing.net/wp/wp-content/uploads/python-logo-1.png) ![aws](https://www.avidsecure.io/wp-content/uploads/2018/08/aws-logo.png) ![pyspark](https://2xbbhjxc6wk3v21p62t8n4d4-wpengine.netdna-ssl.com/wp-content/uploads/2017/06/spark-mllib-logo.png)  

Machine learning and NLP approaches for Amazon comments mining are described in details on my [blog](https://datasciencenote.wordpress.com/).  
Whereas description of the infrustructure set-up and the notebook with code you will find [here]().

## Getting started

These guidlines will help you with setting up AWS EC2 machines **quickly** and at **little cost**. You will learn how to set up Apache Spark cluster on the instances and run your first Spark application on it. Having that done you will be able to connect to the cluster using your local computer via Jupyter Notebook interface.

### Prerequisites

I have done everything having Windows installed on my laptop. If you have another OS - the only difference will be in connection to the EC2 instances, which is easier for MacOS or Linux systems.

What software you need to install before going further:

* [PuTTY](https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html) - To connect to EC2 instances.
* [Git](https://git-scm.com/) - I used Git Bash to access Jupyter Notebook running on master node of Spark cluster. You might use Bash for Windows as well as any other relevat piece of software.
* Spark intuition and understanding of RDD (no advanced knowledge required to follow the guidelines and set up the environment).

This [article](https://databricks.com/blog/2016/06/22/apache-spark-key-terms-explained.html) should be more then enough to understand key term.

#### 1. Setting up AWS EC2 instances

You will need an AWS account to be able to access AWS console. 
In case you don't have it, but would like to create one - here the [link](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).  
All steps below are performed in N.Virginia region (because I had S3 bucket with data already in this region). Feel free to create them in your prefered region.

##### Step 1 (request spot instances):
![first](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/1.%20first%20step.PNG?raw=true)
##### Step 2 (select request type):
'Big data workloads' is the best fit for our exercise.  

![second](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/2.%20big%20data.PNG?raw=true)
##### Step 3 (chose instance type and key pair):
Creating key pair is an optinal step if you already have it created.  
[How to create key pair?](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair)  
If you would like work with another instance type select the one you prefer.

![third](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/3.%20instance%20and%20key.PNG?raw=true) ![fourth](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/4.%20select%20instance%20type.PNG?raw=true)
##### Step 4 (additonal storage, security group & IAM tole):
I've selected 50GB hard drive (8GB is default).  
Security group I've created ('SparkCluster') alows I/O traffic for all IPs. You can make up something more secure, of course :)  
[IAM - Identity and Access Management](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) will allow machines access files on your S3. I selected full access to S3 bucket (as shown below).
![fifth](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/5.%20ebs%2050%20gb%20security%20and%20iam.PNG?raw=true) ![iam](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/6.%20iam%20details.PNG?raw=true)
##### Step 5 (number of instances & the fleet):
I've run 3 instances and ensured that AWS will not make any changes to my fleet while requsting instances (uncheck 'Apply recommendations' and remove all instances types except the one selected by you previously).  

![sixth](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/6.%20target%20capacity%20and%20recommendations.PNG?raw=true)
##### Step 6 (wait for launching and log in):
![sixth](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/7.%20pending%20spot.PNG?raw=true)
![sixth](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/8.%20instances.PNG?raw=true)

Then use PuTTY to log in on all of the by creating connections (chose one to be master node).  
If you are not familiar with how to login - [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html) is detailed guide.

#### 2. Spark cluster (configuration of the master and workers nodes)

> **IMPORTANT!** Perform all steps on all nodes simultaneously (e.g. having all terminals opened on your screen). All nodes should be configure exactly at the same manner. *Exception - definition of workers IPs on master node.*  

![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/work%20screen.png?raw=true)

Before going further with Spark cluster configuration and having our machines up and running, let's update our environments and install important software.
Apache Spark is written in Scala programming language that is being run on JVM – that is why both JDK and Scala are required.  
The code snippet below will update the environment and install Java, Scala and unzip (needed later to work with .zip files).  
It will also download 2.3.2 version of Apache Spark and untar (extract) it to the following location */usr/local*.  

```
sudo apt-get update
sudo apt-get -y install openjdk-8-jre-headless
sudo apt-get -y install scala
sudo apt-get -y install unzip

wget https://www-eu.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz -P ~/Downloads
sudo tar zxvf ~/Downloads/spark-2.3.2-bin-hadoop2.7.tgz  -C /usr/local
```

If you want to assure that both Java and Scala are installed correctly you can run the script below. If working correctly it will return versions of both.

```
java -version
scala -version
```

The next step is crucial as further configurations will depend on it.  
We need to define environmental variables by explicit input of the path to the Spark location.  
The last line of the snippet below will give permission (ownership) to the *ubuntu* user.

```
export SPARK_HOME=/usr/local/spark-2.3.2-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
. ~/.profile

sudo chown -R ubuntu $SPARK_HOME
```

Let's make a copy of Spark environmental set-up file template and make it executable.

```
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
nano $SPARK_HOME/conf/spark-env.sh
```

Chunks from the scrip below are inputs to newly created *spark-env.sh* - do it on all instances separately (e.g. chunk related to the node 1 insert only in *spark-env.sh* on first machine and so on).  

**SPARK_PUBLIC_DNS** sets the public DNS name of the Spark master and workers.  
**SPARK_WORKER_CORES** is total number of cores to be used by executors by each worker. Since we are creating *two workers* machines with *two cores* each, we've got 2x2=4 cores to use. You can treat it as a parallelism that workers have in the cluster

```
export JAVA_HOME=/usr
export SPARK_PUBLIC_DNS=”node 1 public DNS”
export SPARK_WORKER_CORES=4

export JAVA_HOME=/usr
export SPARK_PUBLIC_DNS=”node 2 public DNS”
export SPARK_WORKER_CORES=4

export JAVA_HOME=/usr
export SPARK_PUBLIC_DNS=”node 3 public DNS”
export SPARK_WORKER_CORES=4

...
```

There some additional dependencies that are nice to have to ensure smooth performance of Spark cluster on AWS cloud.  
The following script will download and install them.

Some additional info on the dependencies:  
* [AWS SDK for Java](https://aws.amazon.com/sdk-for-java/)
* [Guava](https://mvnrepository.com/artifact/com.vaadin.external.google/guava/16.0.1.vaadin1)
* [Apache Hadoop Amazon Web Services Support](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.2)

```
wget http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar -P $SPARK_HOME/jars
wget http://central.maven.org/maven2/com/google/guava/guava/16.0.1/guava-16.0.1.jar -P $SPARK_HOME/jars
wget http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.2/hadoop-aws-2.7.2.jar -P $SPARK_HOME/jars
```

Set up configuration for the cluster with previously installed dependencies and define security credentials to be able to access data from you S3 bucket (if you are going to source the data from there, of course).  

[Where’s My Secret Access Key?](https://aws.amazon.com/blogs/security/wheres-my-secret-access-key/)

```
spark.driver.extraClassPath $SPARK_HOME/jars/guava-16.0.1.jar:$SPARK_HOME/jars/aws-java-sdk-1.7.4.jar:$SPARK_HOME/jars/hadoop-aws-2.7.2.jar
spark.executor.extraClassPath $SPARK_HOME/jars/guava-16.0.1.jar:$SPARK_HOME/jars/aws-java-sdk-1.7.4.jar:$SPARK_HOME/jars/hadoop-aws-2.7.2.jar
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.access.key  your_aws_access_key
spark.hadoop.fs.s3a.secret.key  your_aws_secret_access_key
spark.hadoop.fs.s3a.fast.upload true
```


Create and modify file that specifies all your workers.
>**ONLY ON MASTER NODE!**

```
touch $SPARK_HOME/conf/slaves
nano $SPARK_HOME/conf/slaves
worker 1 public DNS
worker 2 public DNS
...
```

Installation of Python 3 and some libraries that I used in the project.  
Second line installs Jupyter Notebook - you can do it on master node only, but installing it on other nodes will not hurt.

```
sudo apt-get -y install python3-dev python3-pip gfortran
pip3 install nose "ipython[notebook]"
pip3 install cython emot twython scipy numpy pandas nltk boto3 seaborn Image
```

The following line will start cluster with all nodes.

```
$SPARK_HOME/sbin/start-all.sh
```

In unlucky case you can get **'permission denied'** kind of issue and you won't be able to start workers from your master node.
You should then locate your *.pem* key you used to create the machine into the **~/.ssh/** folder on master node. This should resolve the issue. I usually use [FileZilla](https://filezilla-project.org/) to drop files onto remote machines.

[How to transfer files between your laptop and Amazon instance?](https://angus.readthedocs.io/en/2014/amazon/transfer-files-between-instance.html)

Another way to tackle it is to manually start all workers. To do that you should run the following line on each worker. This will link them directly to running cluster.

```
$SPARK_HOME/sbin/start-slave.sh spark://MASTER_NODE_PUBLIC_DNS:7077
```

#### 3. Spin up Jupyter and access it from your browser

Final line will spin up Jupyter Notebook - execute it only on master node.  
You can adjust executor and driver memory accordingly to your instances.

```
PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8888" pyspark --packages com.amazonaws:aws-java-sdk-pom:1.10.34 --master spark://MASTER_NODE_PUBLIC_DNS:7077  --executor-memory 6400M --driver-memory 6400M
```

To access Jupyter from your browser you should connect through SSH to the master node. As I mention before I used Git Bash for this. So open Git Bash in the directory where your .pem key is located and run following line.  
**-i** provides identification file to use for public key authentication  
**L** specifies that the given port on the local host is to be forwarded to the given port on AWS. In case something else is running on your 8000 port you can modify it.


```
ssh -i your_key.pem -L 8000:localhost:8888 ubuntu@MASTER_NODE_PUBLIC_DNS
```

![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/12.%20launching%20Jupyter%20from%20local%20machine.PNG?raw=true)

Log in using the token displayed on master node terminal.

![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/13.%20login%20jupyter.PNG?raw=true)

**Spark UI & example of [DAG](https://databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html) vizulization for CountVectorizer job ([full notebook]()):**

![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/spark%20setup%20ui.PNG?raw=true)
![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/spark%20jobs.PNG?raw=true)
![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/dag%20viz.PNG?raw=true)

**Worker pick performance during the task:**
![screen](https://github.com/tkachuksergiy/NLPwithSparkAWS/blob/master/Screenshots/slave%20pick%20performance.png?raw=true)


## Acknowledgment

All great on-line data science and big data communities that gave a lot of inspirations and ideas :)  
StackOverflow and Medium in particular!  

[Michał Korzycki, PhD](https://github.com/MichalKorzycki) who initiated the idea of writing the NLP related thesis. 
