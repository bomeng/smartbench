#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/tpcds/tpcds.conf
. "${root_dir}/bin/functions/load_bench_config.sh"
hibench_dir=${root_dir}/report/query05/spark/monitor.log

enter_bench TPCDSQuery05 ${workload_config} ${current_dir}
show_bannar start

# prepare SQL
HIVEBENCH_SQL_FILE=${root_dir}/sparkbench/tpcds/query/query05.sql

START_TIME=`timestamp`
application_name=query05_${START_TIME}
run_spark_job --app_name "${application_name}" ${HIVEBENCH_SQL_FILE}
END_TIME=`timestamp`

sleep 5
SIZE=0
gen_report ${START_TIME} ${END_TIME} ${SIZE:-0}
sleep 10
python ${root_dir}/bin/functions/sparkParser.py ${INFLUXDB_IP} ${INFLUXDB_PORT} ${INFLUXDB_USER} ${INFLUXDB_PWD} ${INFLUXDB_NAME} ${SPARK_IP_PORT} ${application_name} ${hibench_dir}
show_bannar finish
leave_bench

