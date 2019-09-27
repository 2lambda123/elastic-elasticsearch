#!/bin/bash

# opensuse 15 has a missing dep for systemd 
sudo zypper install -y insserv-compat

# Required by bats
sudo touch /etc/is_vagrant_vm
sudo useradd vagrant

set -e

. .ci/java-versions.properties
RUNTIME_JAVA_HOME=$HOME/.java/$ES_RUNTIME_JAVA
BUILD_JAVA_HOME=$HOME/.java/$ES_BUILD_JAVA

sudo rm -Rfv $HOME/.gradle/init.d
sudo mkdir -p $HOME/.gradle/init.d
sudo rm -Rf /root/.gradle
sudo cp -v .ci/init.gradle $HOME/.gradle/init.d
# levarage the packar cache and init files when ran with root
sudo ln -s /var/lib/jenkins/.gradle /root/.gradle

unset JAVA_HOME

if ! [ -e "/usr/bin/bats" ] ; then 
  git clone https://github.com/sstephenson/bats /tmp/bats
  sudo /tmp/bats/install.sh /usr
fi

sudo bash -c 'cat > /etc/sudoers.d/elasticsearch_vars'  << SUDOERS_VARS
    Defaults   env_keep += "ZIP"
    Defaults   env_keep += "TAR"
    Defaults   env_keep += "RPM"
    Defaults   env_keep += "DEB"
    Defaults   env_keep += "PACKAGING_ARCHIVES"
    Defaults   env_keep += "PACKAGING_TESTS"
    Defaults   env_keep += "BATS_UTILS"
    Defaults   env_keep += "BATS_TESTS"
    Defaults   env_keep += "SYSTEM_JAVA_HOME"
    Defaults   env_keep += "JAVA_HOME"
SUDOERS_VARS
sudo chmod 0440 /etc/sudoers.d/elasticsearch_vars

# Bats tests still use this locationa
rm -Rf /elasticsearch
sudo mkdir -p /elasticsearch/qa/ && sudo chown jenkins /elasticsearch/qa/ && ln -s $PWD/qa/vagrant /elasticsearch/qa/

# sudo sets it's own PATH thus we use env to override that and call sudo annother time so we keep the secure root PATH 
# run with --continue to run both bats and java tests even if one fails
sudo -E env \
  PATH=$BUILD_JAVA_HOME/bin:`sudo bash -c 'echo -n $PATH'` \
  RUNTIME_JAVA_HOME=`readlink -f -n $RUNTIME_JAVA_HOME` \
  --unset=JAVA_HOME \
  SYSTEM_JAVA_HOME=`readlink -f -n $RUNTIME_JAVA_HOME` \
  ./gradlew --parallel $@ --continue destructivePackagingTest

