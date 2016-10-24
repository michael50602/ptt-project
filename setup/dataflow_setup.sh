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
#! be aware of the version of oauth2client
sudo pip install --upgrade google-api-python-client

# install cloud sdk
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-130.0.0-linux-x86_64.tar.gz
tar zxvf google-cloud-sdk-130.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
./google-cloud-sdk/bin/gcloud init
# install dataflow sdk
sudo pip install google-cloud-dataflow oauth2client==3.0.0.


