cd /srv/libvirt-workdir && mkdir data && cd data
wget https://www.lrde.epita.fr/~ricou/pybd/projet/boursorama.tar
tar -xvf bousorama.tar
rm boursorama.tar
(cd /srv/libvirt-workdir/bourse/docker/analyzer && make fast);
(cd /srv/libvirt-workdir/bourse/docker/dashboard && make fast);
