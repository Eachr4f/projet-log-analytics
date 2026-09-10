#!/bin/bash

BASE_URL="http://localhost:8090"
ROUTES=("/" "/api/users" "/api/orders" "/notfound" "/error" "/slow")
WEIGHTS=(30 25 25 10 5 5)  # pourcentages approximatifs de répartition

echo "Démarrage de la génération de trafic... (Ctrl+C pour arrêter)"

while true; do
    RAND=$((RANDOM % 100))
    CUMUL=0
    for i in "${!ROUTES[@]}"; do
        CUMUL=$((CUMUL + WEIGHTS[i]))
        if [ $RAND -lt $CUMUL ]; then
            ROUTE=${ROUTES[i]}
            break
        fi
    done

    curl -s -o /dev/null "$BASE_URL$ROUTE"
    echo "Requête envoyée: $ROUTE"

    sleep 0.$((RANDOM % 5 + 1))
done
