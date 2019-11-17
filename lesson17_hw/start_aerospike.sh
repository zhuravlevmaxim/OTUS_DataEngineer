docker rm db
docker run -tid --name db -p 3000:3000 aerospike/aerospike-server
