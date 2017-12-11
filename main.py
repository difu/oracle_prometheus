from __future__ import print_function
from prometheus_client import start_http_server, Summary, Counter, Gauge
import time


import cx_Oracle

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
NUMBER_OF_SESSIONS = Gauge('number_of_sessions', 'Number of sessions', ['host', 'sid', 'con_id', 'username'])
WAIT_CLASSES = Gauge('wait_class', 'Wait Class', ['host', 'sid', 'wait_class'])

hostname = 'UNKNOWN'
database_sid = 'UNKNOWN'


def get_db_details(conn):
    global hostname
    global database_sid
    cursor = conn.cursor()
    cursor.execute("select db.name, inst.host_name from v$database db, v$instance inst")
    row = cursor.fetchone()
    database_sid = row[0]
    hostname = row[1]
    cursor.close()


#@REQUEST_TIME.time()
def scrape_wait_classes(conn):
    cursor = conn.cursor()
    # Statement taken from http://www.oaktable.net/content/wait-event-and-wait-class-metrics-vs-vsystemevent
    cursor.execute("select n.wait_class, round(m.time_waited/m.INTSIZE_CSEC,3) AAS"
                   " from v$waitclassmetric  m, v$system_wait_class n where m.wait_class_id=n.wait_class_id"
                   " and n.wait_class != 'Idle' union select  'CPU', round(value/100,3) AAS from v$sysmetric"
                   " where metric_name='CPU Usage Per Sec' and group_id=2 "
                   "union select 'CPU_OS', round((prcnt.busy*parameter.cpu_count)/100,3) - aas.cpu from"
                   "( select value busy from v$sysmetric "
                   "where metric_name='Host CPU Utilization (%)' and group_id=2 ) prcnt,"
                   "( select value cpu_count from v$parameter where name='cpu_count' )  parameter,"
                   "( select  'CPU', round(value/100,3) cpu from v$sysmetric"
                   " where metric_name='CPU Usage Per Sec' and group_id=2) aas")
    for result in cursor:
        print (result)
        WAIT_CLASSES.labels(hostname, database_sid, result[0]).set(result[1])
    cursor.close()


#@REQUEST_TIME.time()
def count_sessions(conn):
    cursor = conn.cursor()
    cursor.execute("select con_id, username, count(*) from v$session where type ='USER' group by username, con_id")
    for result in cursor:
        print (result)
        NUMBER_OF_SESSIONS.labels(hostname, database_sid, result[0], result[1]).set(result[2])
    cursor.close()


if __name__ == '__main__':

    connection = cx_Oracle.connect("sys", "Oradoc_db1",
                                   "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ORCLCDB.localdomain)))", mode = cx_Oracle.SYSDBA)

    # Start up the server to expose the metrics.
    start_http_server(8000)

    get_db_details(connection)

    while True:
        time.sleep(1)
        count_sessions(connection)
        scrape_wait_classes(connection)

