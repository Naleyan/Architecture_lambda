#!/bin/bash

echo "🚀 Lancement de l'architecture Lambda..."

# Réseau (si pas déjà créé)
docker network create mynetwork 2>/dev/null

echo "▶️ Kafka..."
cd kafka
docker-compose up -d

echo "▶️ Spark..."
cd ../spark
docker-compose up -d --build

echo "▶️ Consumer..."
cd ../consumer
docker-compose up -d --build

echo "▶️ Producer..."
cd ../producer
docker-compose up -d --build

echo "✅ Tous les services sont lancés !"

echo "📊 Interfaces disponibles :"
echo "- Kafka (Kafdrop) : http://localhost:19000"
echo "- Spark UI        : http://localhost:9090"