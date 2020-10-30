# Project: Data Lake with Spark
This project creates an 'Extract, Transform, and Load' pipeline, also called an ETL pipeline, for the company Sparkify that extracts data from S3, transforms the data in Apache Spark into a set of fact and dimensional tables, and store the new data in another S3 bucket. This will give their analytics team the ability to easily find insights into what songs their users are listening to. This move from Data Warehouse to Data Lake will give the analytics team at Sparkify faster transformations, and reduceed cost of operation.


## Prerequisites
* Apache Spark
* PySpark
* AWS IAM User
* AWS IAM Role
* AWS EMR with Spark
* AWS S3


## Launch
Run the etl.py file to load and insert into the new fact and dimension tables.

### Setting up the aws environment and cli
Create a S3 bucket.  
In AWS EMR, go to notebooks and select the Change Data Lake cluster to create a EMR cluster.  
Check the notebook and not the cluster ID.  
Open an ssh with the correct address:  
Run these two to open the ssh:  
```
ssh -i ~/.ssh/pem-file.pem hadoop@ec2-[ADDRESS].us-west-2.compute.amazonaws.com -ND 8157  
aws emr ssh --cluster-id j-2MD2LOY3F7AO4 --key-pair-file ~/.ssh/Spark-cluster-new-key-pair.pem  
```

If you need to [change kernel to pyspark](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/  ) inside EMR cli, do the following:  

```
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh  
```
Check with  
```
$ pyspark  
```

To run the etl.py file you need to submit file to cluster:  
```
$ which spark-submit  
/usr/bin/spark-submit  
```
Submit file with this command:  
```
/usr/bin/spark-submit --master yarn ./etl.py  
```

### Jupyter Notebook in EMR
To load a jupyter notebook to an EMR cluster create a notebook in the EMR consol and use the 'open in Jupyter' option, not the 'JupyterLab'
option. From there you use the upload function in the window where you choose your notebook. This will add a notebook to the EMR cluster. 


## License

MIT License
