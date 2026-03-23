#!/bin/bash

echo "🛑 Arrêt de l'architecture Lambda..."

echo "⏹️ Producer..."
cd producer
docker-compose down

echo "⏹️ Consumer..."
cd ../consumer
docker-compose down

echo "⏹️ Spark..."
cd ../spark
docker-compose down

echo "⏹️ Kafka..."
cd ../kafka
docker-compose down

echo "🧹 Nettoyage (optionnel)..."
docker system prune -f

echo "✅ Tous les services sont arrêtés."