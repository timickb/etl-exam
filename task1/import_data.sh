#!/bin/sh

ydb \                                                                                                              :(
--endpoint grpcs://ydb.serverless.yandexcloud.net:2135 \
--database /ru-central1/b1g7r0rgindmq8h3ndfp/etnqufilhqh8ons1q76u \
--sa-key-file authorized_key.json \
import file csv \
--path transactions_v2 \
--delimiter "," \
--skip-rows 1 \
--null-value "" \
transactions_v2.csv
