# this shell script should be executed after:
# 1. creating project 2. enabling api 3. installing cloud SDK 4. create a compute engine 
gcloud components update beta

# install hbase shell(depend on java) ! mind the version of java
sudo apt-get upgrate
sudo apt-get install openjdk-8-jre-headless # install jdk

#! this command only for install not on google compute engine
# gcloud beta auth application-default login 

# download Apache HBase to a directory
curl -f -O http://storage.googleapis.com/cloud-bigtable/hbase-dist/hbase-1.2.1/hbase-1.2.1-bin.tar.gz
tar xvf hbase-1.2.1-bin.tar.gz

mkdir -p hbase-1.2.1/lib/bigtable # mind the version
curl http://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.2/0.9.3/bigtable-hbase-1.2-0.9.3.jar -f -o hbase-1.2.1/lib/bigtable/bigtable-hbase-1.2-0.9.3.jar
curl http://repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/1.1.33.Fork19/netty-tcnative-boringssl-static-1.1.33.Fork19.jar -f -o hbase-1.2.1/lib/netty-tcnative-boringssl-static-1.1.33.Fork19.jar
export JAVA_HOME=$(update-alternatives --list java | tail -1 | sed -E 's/\/bin\/java//')

sudo apt-get install maven
