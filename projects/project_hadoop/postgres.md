docker run --name postgresql -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres

python map_only.py dataset/TR_Products.csv

docker cp map_only.py namenode:./map_only.py
docker cp dataset/TR_Products.csv namenode:./TR_Products.csv

<!-- masuk ke dalam namenode di Terminal -->
docker exec -it namenode bash

hdfs dfs -ls /
hdfs dfs -put TR_Products.csv /user/root/

<!-- if there is no python installed on the container. -->
apt-get update -y
apt-get install python3
apt-get install python3-pip

<!-- install the library -->
