# use google cloude api on each node to access file on google storage
> ref: https://cloud.google.com/storage/docs/json_api/v1/objects/get
> environment setting ref: https://cloud.google.com/compute/docs/tutorials/python-guide
 the object value of request is the path under the bucket

# connect dataflow to bigtable
> set up a bigtable instance ref: https://cloud.google.com/bigtable/docs/creating-instance
> install Cloud SDK to access bigtable 
> bigtable is based on HBase so you need to install HBase first. Mind your java version and hbase version
> set the config file in hbase path: ptt-project/setup/hbase-1.2.1/conf/hbase-site.xml
> add environment path variable to hbase-env.sh
