import json
from pprint import pprint
import csv
from collections import namedtuple
from itertools import imap
import re
from datetime import datetime
from influxdb import InfluxDBClient
import subprocess
import time
import sys

def sparkApplicaiton(ipPort, applicationName, client):
    print("Fetching current application information ...")
    url = 'http://' + ipPort + '/api/v1/applications'
    applications = ""
    try:
        applications = subprocess.check_output(['curl',url])
    except:
        print "Cannot fetch application level metric."
    data = json.loads(applications)
    for x in data:
        if x['name'] == applicationName:
            applicationID = x['id']
            sparkStage(ipPort, applicationID, client)
            return applicationID
            break

def sparkStage(ipPort, applicationID, client):
    print("Parsing stage information ...")
    applicationList = 'http://' + ipPort + '/api/v1/applications/' + applicationID + '/stages'
    stages = ""
    try:
        stages = subprocess.check_output(['curl',applicationList])
    except:
        print "Cannot fetch stage level metric."
    data = json.loads(stages)
    for x in data:
        res_data = []
        sparkTask(ipPort, str(x['stageId']), str(x['attemptId']), applicationID, res_data)
        try:
            client.write_points(res_data)
        except Exception as ex:
            print ex

def sparkTask(ipPort, stageID, attamps, applicationID, res_data):
    print("Parse task information.")
    stageList = 'http://' + ipPort + '/api/v1/applications/' + applicationID + '/stages/' + stageID + '/' + attamps + '/taskList'
    taskList = ""
    try:
        taskList = subprocess.check_output(['curl',stageList])
    except:
        print "Cannot fetch task level metric."
    data = json.loads(taskList)
    for x in data:
        tags = {'applicationID' : applicationID,'taskId' : int(x['taskId']), 'attempt' : int(x['attempt']), 'host' : str(x['host']), 'executorId' : int(x['executorId'])}
        unixLaunchTime = time.strptime(x['launchTime'],'%Y-%m-%dT%H:%M:%S.%f%Z')
        launchTime = time.strftime('%Y-%m-%dT%H:%M:%SZ', unixLaunchTime)
        fields ={'launchTime' : launchTime, \
        'duration' : int(x['duration']), \
        'status' : x['status'], \
        'taskLocality' : x['taskLocality'], \
        'speculative' : bool(x['speculative']), \
        'executorDeserializeTime' : int(x['taskMetrics']['executorDeserializeTime']), \
        'executorDeserializeCpuTime' : int(x['taskMetrics']['executorDeserializeCpuTime']), \
        'executorRunTime' : int(x['taskMetrics']['executorRunTime']), \
        'executorCpuTime' : int(x['taskMetrics']['executorCpuTime']), \
        'resultSize' : int(x['taskMetrics']['resultSize']), \
        'jvmGcTime' : int(x['taskMetrics']['jvmGcTime']), \
        'resultSerializationTime' : int(x['taskMetrics']['resultSerializationTime']), \
        'memoryBytesSpilled' : int(x['taskMetrics']['memoryBytesSpilled']), \
        'diskBytesSpilled' : int(x['taskMetrics']['diskBytesSpilled']), \
        'inputbytesRead' : int(x['taskMetrics']['inputMetrics']['bytesRead']), \
        'inputrecordsRead' : int(x['taskMetrics']['inputMetrics']['recordsRead']), \
        'outputbytesWritten' : int(x['taskMetrics']['outputMetrics']['bytesWritten']), \
        'outputrecordsWritten' : int(x['taskMetrics']['outputMetrics']['recordsWritten']), \
        'shuffleReadremoteBlocksFetched' : int(x['taskMetrics']['shuffleReadMetrics']['remoteBlocksFetched']), \
        'shuffleReadlocalBlocksFetched' : int(x['taskMetrics']['shuffleReadMetrics']['localBlocksFetched']), \
        'shuffleReadfetchWaitTime' : int(x['taskMetrics']['shuffleReadMetrics']['fetchWaitTime']), \
        'shuffleReadremoteBytesRead' : int(x['taskMetrics']['shuffleReadMetrics']['remoteBytesRead']), \
        'shuffleReadlocalBytesRead' : int(x['taskMetrics']['shuffleReadMetrics']['localBytesRead']), \
        'shuffleReadrecordsRead' : int(x['taskMetrics']['shuffleReadMetrics']['recordsRead']), \
        'shuffleWritebytesWritten' : int(x['taskMetrics']['shuffleWriteMetrics']['bytesWritten']), \
        'shuffleWritewriteTime' : int(x['taskMetrics']['shuffleWriteMetrics']['writeTime']), \
        'shuffleWriterecordsWritten' : int(x['taskMetrics']['shuffleWriteMetrics']['recordsWritten'])
        }
        row = {'measurement' : 'spark', 'tags' : tags, 'time' : launchTime, 'fields' : fields}
        res_data.append(row)

def influxDBWriter(line, timestamp, hostname, applicationID, data):
    if "cpu/" in line:
        tmp = line.split('(')
        index = tmp[1].split(',')
        tags = {'hostname' : hostname, 'applicationID' : applicationID, 'label' : index[0].split("=")[1].split('\'')[1]}
        fields = {'user' : float(index[1].split("=")[1]), 'nice' : float(index[2].split("=")[1]), 'system' : float(index[3].split("=")[1]), 'idle' : float(index[4].split("=")[1]), 'iowait' : float(index[5].split("=")[1]), 'irq' : float(index[6].split("=")[1]), 'softirq' : float(index[7].split("=")[1].strip(")}\n"))}
        row = {'measurement' : 'cpu', 'tags' : tags, 'time' : timestamp, 'fields' : fields}
        data.append(row)
    elif "disk/" in line:
        tmp = line.split('(')
        index = tmp[1].split(',')
        tags = {'hostname' : hostname, 'applicationID' : applicationID, 'label' : index[0].split("=")[1].split('\'')[1]}
        fields = {'io_read' : float(index[1].split("=")[1]), 'bytes_read' : float(index[2].split("=")[1]), 'time_spent_read' : float(index[3].split("=")[1]), 'io_write' : float(index[4].split("=")[1]), 'bytes_write' : float(index[5].split("=")[1]), 'time_spent_write' : float(index[6].split("=")[1].strip(")}\n"))}
        row = {'measurement' : 'disk', 'tags' : tags, 'time' : timestamp, 'fields' : fields}
        data.append(row)
    elif "net/" in line:
        tmp = line.split('(')
        index = tmp[1].split(',')
        tags = {'hostname' : hostname, 'applicationID' : applicationID, 'label' : index[0].split("=")[1].split('\'')[1]}
        fields = {'recv_bytes' : int(index[1].split("=")[1]), 'recv_packets' : int(index[2].split("=")[1]), 'recv_errs' : int(index[3].split("=")[1]), 'recv_drop' : int(index[4].split("=")[1]), 'send_bytes' : int(index[5].split("=")[1]), 'send_packets' : int(index[6].split("=")[1]), 'send_errs' : int(index[7].split("=")[1]), 'send_drop' : int(index[8].split("=")[1].strip(")}\n"))}
        row = {'measurement' : 'network', 'tags' : tags, 'time' : timestamp, 'fields' : fields}
        data.append(row)
    elif "memory/" in line:
        tmp = line.split('(')
        index = tmp[1].split(',')
        tags = {'hostname' : hostname, 'applicationID' : applicationID, 'label' : index[0].split("=")[1].split('\'')[1]}
        fields = {'total' : int(index[1].split("=")[1]), 'used' : int(index[2].split("=")[1]), 'buffer_cache' : int(index[3].split("=")[1]), 'free' : int(index[4].split("=")[1]), 'map' : int(index[5].split("=")[1].strip(")}\n"))}
        row = {'measurement' : 'memory', 'tags' : tags, 'time' : timestamp, 'fields' : fields}
        data.append(row)

def hibench(dir, applicationID, client):
    print("Parsing hibench information ...")
    data = []
    with open(dir, mode="rb") as osInfo:
        index = 0
        for line in osInfo:
            if index >= 4:
                timestamp = "null"
                hostname = "null"
                info = line.split('),')
                for i in info:
                    if "timestamp" in i:
                        tmp = float(i.split(',')[0].split(':')[1])
                        timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(tmp))
                    elif "hostname" in i:
                        hostname = i.split(',')[0].split(':')[1].split('\'')[1]
                for i in info:
                    if "timestamp" in i or "hostname" in i:
                        influxDBWriter(i[i.find(',')+1:], timestamp, hostname, applicationID, data)
                    else:
                        influxDBWriter(i, timestamp, hostname, applicationID, data)
            index += 1
    try:
        client.write_points(data)
    except Exception as ex:
        print ex

def main():
	#influxDB ip address
    ip = sys.argv[1]
    #influxDB port
    port = sys.argv[2]
    #influxDB user name
    user = sys.argv[3]
    #influxDB user password
    pwd = sys.argv[4]
    #influxDB database name
    db = sys.argv[5]
    #Spark history server ip:port
    historyServer = sys.argv[6]
    #Spark application name
    applicationName = sys.argv[7]
    #hibench monitor directory
    hibenchDir = sys.argv[8]
    
    #initial influxdb
    print "initial connection to InfluxDB."
    client = InfluxDBClient(ip, int(port), user, pwd, db)
    client.create_database(db)
    #Application ID and parser data from Spark history server
    print "Parsing Spark history data ...."
    applicaitonId = sparkApplicaiton(historyServer, applicationName, client)
    #Parse hibench data
    print "Parsing Hibench monitor data ..."
    try:
        hibench(hibenchDir, applicaitonId, client)
    except Exception as ex:
        print ex

if __name__ == "__main__":                
    main()