# Install docker docker. Not podman

sudo yum install -y yum-utils
sudo yum install sysstat

sudo yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo

sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl start docker

sudo groupadd docker
sudo usermod -aG docker $USER

newgrp docker
docker run hello-world
