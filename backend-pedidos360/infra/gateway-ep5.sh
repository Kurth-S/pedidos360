#!/usr/bin/env bash
# EP5 — agrega al API Gateway existente las rutas de ms-kafka-admin (8084) y ms-analitica (8085).
# Ejecutar en CloudShell (región us-east-1). Antes: levantar los servicios en la EC2 y abrir 8084-8085 en su security group.
#
#   export API_ID=61m2ifom2j          # id de pedidos360-api
#   export EC2_IP=<ip-publica-de-la-ec2>
#   bash gateway-ep5.sh
set -euo pipefail
: "${API_ID:?Define API_ID}"
: "${EC2_IP:?Define EC2_IP}"

# Reutiliza el authorizer JWT que ya tiene la API (el primero que exista)
AUTH_ID=$(aws apigatewayv2 get-authorizers --api-id "$API_ID" --query 'Items[0].AuthorizerId' --output text)
echo "Authorizer JWT: $AUTH_ID"

# Integraciones HTTP_PROXY: el path completo viaja hasta el servicio ({proxy} = resto de la ruta)
INT_KAFKA=$(aws apigatewayv2 create-integration --api-id "$API_ID" \
  --integration-type HTTP_PROXY --integration-method ANY --payload-format-version 1.0 \
  --integration-uri "http://${EC2_IP}:8084/api/kafka/{proxy}" --query IntegrationId --output text)
INT_ANALITICA=$(aws apigatewayv2 create-integration --api-id "$API_ID" \
  --integration-type HTTP_PROXY --integration-method ANY --payload-format-version 1.0 \
  --integration-uri "http://${EC2_IP}:8085/api/analitica/resumen" --query IntegrationId --output text)

aws apigatewayv2 create-route --api-id "$API_ID" --route-key 'ANY /api/kafka/{proxy+}' \
  --target "integrations/${INT_KAFKA}" --authorization-type JWT --authorizer-id "$AUTH_ID" >/dev/null
aws apigatewayv2 create-route --api-id "$API_ID" --route-key 'GET /api/analitica/resumen' \
  --target "integrations/${INT_ANALITICA}" --authorization-type JWT --authorizer-id "$AUTH_ID" >/dev/null

# CORS: no se modifica. Estas rutas se prueban con Postman/curl (sin navegador). Si algún día el frontend las llama,
# agrega PATCH y DELETE a AllowMethods de la API (Gateway > CORS) conservando los orígenes que ya tienes.

echo "Listo:  https://${API_ID}.execute-api.us-east-1.amazonaws.com/api/kafka/topics"
echo "        https://${API_ID}.execute-api.us-east-1.amazonaws.com/api/analitica/resumen"
