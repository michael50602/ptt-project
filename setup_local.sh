# install pip 
sudo apt-get install python-pip
# upgrade pip
export LC_ALL=C
sudo pip install --upgrade pip
sudo pip install virtualenv
virtualenv $1
source $1/bin/activate
sudo pip install numpy
sudo pip install ipython


#install google cloud dataflow 
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-130.0.0-linux-x86_64.tar.gz
tar zxvf google-cloud-sdk-130.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
./google-cloud-sdk/bin/gcloud init
sudo pip install google-cloud-dataflow
#install py-dataflow
git clone -b python-sdk https://github.com/apache/incubator-beam.git
cd incubator-beam/sdks/
tar cfz ~/python.tgz python/
sudo pip install ~/python.tgz
#rrm -rf DataflowPythonSDK-0.2.7/

# install gcsfuse
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install gcsfuse

