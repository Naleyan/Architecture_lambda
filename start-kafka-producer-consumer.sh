cd kafka
docker compose -f docker-compose.yml up -d
cd ../consumer
docker compose -f docker-compose.yml up -d
cd ../producer
docker compose -f docker-compose.yml up -d