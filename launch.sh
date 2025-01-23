#!/bin/bash

APP_SERVICE_CMD="./target/release/app-service -c config/service/app_service.yml"
PNL_SERVICE_CMD="./target/release/pnl-service -c config/service/pnl_service.yml"
BROKER_SERVICE_CMD="./target/release/broker-service -c config/service/broker_service.yml"
ORDER_SERVICE_CMD="./target/release/order-service -c config/service/order_service.yml"
LOGGER_SERVICE_CMD="./target/release/logger-service -c config/service/logger_service.yml"
DATAFEED_SERVICE_CMD="./target/release/datafeed-service -c config/service/datafeed_service.yml"
ALGO_CMD_SERVICE="./target/release/algo-service -c config/service/algo_service.yml"

$APP_SERVICE_CMD &
$PNL_SERVICE_CMD &
$BROKER_SERVICE_CMD &
$ORDER_SERVICE_CMD &
$LOGGER_SERVICE_CMD &
$ALGO_CMD_SERVICE &
$DATAFEED_SERVICE_CMD &