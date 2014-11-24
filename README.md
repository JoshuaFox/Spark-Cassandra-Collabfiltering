spark-cassandra-collabfiltering
===============================

Illustrates:
- Collaborative filtering with MLLib
- on Spark 
- based on data in Cassandra
- a small data set of employees rating the companies they work at

Does not include
- Clustering of Spark or Cassandra

To setup on Ubuntu 14.04
- Get JDK Java8 with
sudo apt-get install oracle-java8-installer

- Get Spark from http://spark.apache.org/downloads.html 
- Download 1.1.0 for Hadoop 2.4. We will not be using Hadoop/HDFS/HBase, but rather Cassandra.
- Untar the spark tarball. (I put it in ~/dev.)
- Test the installation with 
./bin/run-example SparkPi
- QuickStart has more on setup  https://spark.apache.org/docs/1.1.0/quick-start.html

- Get Eclipse
- Download Eclipse Luna 4.4.1 Ubuntu 64 Bit (or 32 Bit) from https://eclipse.org/downloads/
- Set your Java 8 JDK as the default JDK. 

Untar, Run, Add m2e Maven Integration

Menu Help -> Install New Software…
Add this repository http://download.eclipse.org/technology/m2e/releases f
Check Maven Integration for Eclipse, install

Create new Project -> Maven Project

- Use the included pom.xml.
- Right-click on pom.xml, choose build, target install.
- This will now download Spark jars; it will take a while.
- Itwill also set your Eclipse project's  source level to Java 8.
 

- Get Cassandra
- Instructions here http://www.datastax.com/documentation/cassandra/2.0/cassandra/install/installDeb_t.html
- Run Cassandra
sudo /usr/bin/cassandra
- This is for development only. We will be running Cassandra and Spark locally with console, rather than remotely in a cluster as daemon/service.
- For a Cassandra command line client, run  /usr/bin/cqlsh

- Create schema by running attached SQL
- In workspace root, run
cqlsh -f ./collabfilter/src/sql/collab_filter_schema.sql
- Load data
cqlsh -f ./collabfilter/src/sql/load_data.sql


- Notes on the code: You can find a collaborative filtering tutorial for Spark at https://spark.apache.org/docs/1.1.0/mllib-collaborative-filtering.html 


Guava 15 warning

Japi package added? Removed?
http://stackoverflow.com/questions/27110458/what-changed-in-the-cassandra-spark-integration-connector-package


