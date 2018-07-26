from __future__ import print_function
from prometheus_client import start_http_server, Summary, Counter, Gauge
import time


import cx_Oracle

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
NUMBER_OF_SESSIONS = Gauge('number_of_sessions', 'Number of sessions', ['host', 'sid', 'con_id', 'service_name', 'username'])
WAIT_CLASSES = Gauge('wait_class', 'Wait Class', ['host', 'sid', 'wait_class'])
TABLESPACE_TOTAL_USAGE = Gauge('tablespace_total_usage', 'Tablespace Usage', ['host', 'sid', 'con_id', 'tablespace_name'])

hostname = 'UNKNOWN'
database_sid = 'UNKNOWN'


def get_db_details(conn):
    global hostname
    global database_sid
    global is_cdb
    cursor = conn.cursor()
    cursor.execute("select db.name, inst.host_name, cdb from v$database db, v$instance inst")
    row = cursor.fetchone()
    database_sid = row[0]
    hostname = row[1]
    is_cdb = (row[2] == "YES")
    cursor.close()


def scrape_wait_classes(conn):
    cursor = conn.cursor()
    # Statement taken from http://www.oaktable.net/content/wait-event-and-wait-class-metrics-vs-vsystemevent
    cursor.execute("select replace(replace(lower(n.wait_class),' ','_'),'/',''), round(m.time_waited/m.INTSIZE_CSEC,3) AAS"
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


def scrape_sessions(conn):
    cursor = conn.cursor()
    cursor.execute("select con_id, username, service_name, count(*) "
                   "from v$session where type ='USER' group by username, con_id, service_name")
    for result in cursor:
        print (result)
        NUMBER_OF_SESSIONS.labels(hostname, database_sid, result[0], result[1], result[2]).set(result[3])
    cursor.close()


def scrape_tablespace_usage(conn):
    cursor = conn.cursor()
    # Statement taken from http://www.dba-oracle.com/t_tablespace_script.htm
    cursor.execute("select"
                   "   a.con_id,"
                   "   a.tablespace_name,"
                   "   a.bytes_alloc \"TOTAL ALLOC\","
                   "   a.physical_bytes/(1024*1024) \"TOTAL PHYS ALLOC (MB)\","
                   "   nvl(b.tot_used,0)/(1024*1024) \"USED (MB)\","
                   "   (nvl(b.tot_used,0)/a.bytes_alloc)*100 \"% USED\" "
                   "from"
                   "   (select "
                   "      tablespace_name,"
                   "      con_id,"
                   "      sum(bytes) physical_bytes,"
                   "      sum(decode(autoextensible,'NO',bytes,'YES',maxbytes)) bytes_alloc"
                   "    from" 
                   "      cdb_data_files"
                   "    group by "
                   "       con_id, tablespace_name ) a,"
                   "   (select "
                   "      tablespace_name,"
                   "      con_id,"
                   "      sum(bytes) tot_used"
                   "    from "
                   "      cdb_segments"
                   "    group by "
                   "      con_id, tablespace_name ) b "
                   "where "
                   "   a.tablespace_name = b.tablespace_name (+) and"
                   "   a.con_id = b.con_id (+) "
                   "and "
                   "   a.tablespace_name not in" 
                   "   (select distinct "
                   "       tablespace_name" 
                   "    from "
                   "       cdb_temp_files) "
                   "and "
                   "   a.tablespace_name not like 'UNDO%' "
                   "order by 1,2")
    for result in cursor:
        print (result)
        total_alloc = result[2]
        if total_alloc is not None:
            total_alloc = float(total_alloc)
        else:
            total_alloc = 0
        TABLESPACE_TOTAL_USAGE.labels(hostname, database_sid,  result[0],  result[1]).set(total_alloc)

    cursor.close()


if __name__ == '__main__':

    connection = cx_Oracle.connect("sys", "Oradoc_db1",
                                   "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ORCLCDB.localdomain)))", mode = cx_Oracle.SYSDBA)

    # Start up the server to expose the metrics.
    start_http_server(8000)

    get_db_details(connection)

    while True:
        scrape_sessions(connection)
        scrape_wait_classes(connection)
        scrape_tablespace_usage(connection)
        time.sleep(10)

