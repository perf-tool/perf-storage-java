#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

cd "$(dirname "$0")"

cd ..

PERF_HOME=`pwd`

echo $PERF_HOME

mkdir $PERF_HOME/logs

# memory option
if [ ! -n "$HEAP_MEM" ]; then
  HEAP_MEM="1G"
fi
if [ ! -n "$DIR_MEM" ]; then
  DIR_MEM="1G"
fi

# mem option
JVM_OPT="-Xmx${HEAP_MEM} -Xms${HEAP_MEM} -XX:MaxDirectMemorySize=${DIR_MEM}"

# gc option
if [ ! -n "${GC_THREADS}" ]; then
  GC_THREADS=1
fi
JVM_OPT="${JVM_OPT} -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions"
JVM_OPT="${JVM_OPT} -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=${GC_THREADS} -XX:ConcGCThreads=${GC_THREADS}"

# gc log option
JVM_OPT="${JVM_OPT} -Xlog:gc*=info,gc+phases=debug:$PERF_HOME/logs/gc.log:time,uptime:filecount=10,filesize=100M"

# skywalking java agent option
if [ -n "${SW_AGENT_ENABLE}" ]; then
  if [ ! -n "${SW_SERVICE_NAME}" ]; then
    SW_SERVICE_NAME="perf-storage"
  fi
  if [ ! -n "${SW_COLLECTOR_URL}" ]; then
    SW_COLLECTOR_URL="localhost:11800"
  fi
  # ignore springmvc agent plugin
  rm -rf /opt/perf/skywalking-agent/plugins/apm-springmvc-annotation*
  AGENT_OPT="-javaagent:/opt/perf/skywalking-agent/skywalking-agent.jar -Dskywalking.agent.service_name=${SW_SERVICE_NAME} -Dskywalking.collector.backend_service=${SW_COLLECTOR_URL}"
fi

java $AGENT_OPT $JAVA_OPT $JVM_OPT -Dlog4j.configurationFile=conf/log4j2.xml -classpath $PERF_HOME/lib/*:$PERF_HOME/perf-storage.jar:$PERF_HOME/conf/*  com.github.perftool.storage.Main >>$PERF_HOME/logs/stdout.log 2>>$PERF_HOME/logs/stderr.log
