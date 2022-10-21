# importing csv module
import csv
from datetime import datetime
from dateutil import tz
from influxdb import InfluxDBClient
import logging

# csv file name
FILENAME = "user1.csv"
USER1_AVG_BPM = 55
USER2_AVG_BPM = 85
USER1_NAME = "Paul"
USER2_NAME = "Helen"

INFLUXDB_NAME = 'bpstats'
INFLUXDB_HOST = 'localhost'  # Add your INFLUXDB HOST
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'admin'
INFLUXDB_PASSWORD = ''
INFLUXDB_LOG_DIR = None  # './data/influxdb'
INFLUXDB_MEASUREMENT = 'bpstats_'

IFLUXDB_DATALIST = [
    "diastolic",
    "systolic",
    "bpm"
]

TIMESTAMP_OFFSET = 0
DIASTOLIC_OFFSET = TIMESTAMP_OFFSET + 1
SYSTOLIC_OFFSET = DIASTOLIC_OFFSET + 1
BPM_OFFSET = SYSTOLIC_OFFSET + 1
IHB_OFFSET = BPM_OFFSET + 1
MOV_OFFSET = IHB_OFFSET + 1
USER_OFFSET = MOV_OFFSET + 1

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s %(filename)s:%(lineno)s %(levelname)-8s %(message)s',
                datefmt='%d-%b-%y %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel("DEBUG")

# initializing the titles and rows list
fields = []
rows = []

# reading csv file
with open(FILENAME, 'r') as csvfile:
    # creating a csv reader object
    csvreader = csv.reader(csvfile)
    
    # extracting field names through first row
    fields = next(csvreader)
    fields.append("username")
    fields.append("User1 Off")
    fields.append("User2 Off")
    fields.append("Delta")

    # extracting each data row one by one
    for row in csvreader:
        # Don't use data if the cuff detected too much movement.
        if int(row[MOV_OFFSET]) == 0 :
            # Influx stores timestamps as UTC.
            # Convert from local to UTC.
            to_zone = tz.gettz('UTC')
            db_time = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            db_time = db_time.astimezone(to_zone)
            #print("Time changed from: " + row[0] + " to: " + str(db_time))
            row[0] = db_time
            #determine whose data it is and append name
            user1_bpm_offset = abs(USER1_AVG_BPM - int(row[BPM_OFFSET]))
            user2_bpm_offset = abs(USER2_AVG_BPM - int(row[BPM_OFFSET]))
            if user1_bpm_offset < user2_bpm_offset :
                username = USER1_NAME
            else:
                username = USER2_NAME

            # Add the username to the end of the row
            row.append(username)
            row.append(user1_bpm_offset)
            row.append(user2_bpm_offset)
            row.append(abs(user1_bpm_offset - user2_bpm_offset))
            rows.append(row)
        else:
            print("Row dropped due to movement detected:" + str(row))

    # get total number of rows
    #print("Total no. of rows: %d"%(csvreader.line_num))

# printing the field names
#print('Field names are:' + ', '.join(field for field in fields))

# printing first 5 rows
#print('\nFirst 5 rows are:\n')
#for row in rows[:5]:
print("               ",end='')
for col in fields:
    print("%10s"%col,end=" ")
print('\n')
for row in rows:
    # parsing each column of a row
    for col in row:
        print("%10s"%col,end=" "),
    print('\r')

# Now write the data to the influx database
influx_client = InfluxDBClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, INFLUXDB_NAME)

# Write each data point to the influx DB
for row in rows:
    ifx_fields = {}
    ifx_fields['diastolic'] = int(row[DIASTOLIC_OFFSET])
    ifx_fields['systolic'] = int(row[SYSTOLIC_OFFSET])
    ifx_fields['bpm'] = int(row[BPM_OFFSET])
    ifx_fields['ihb'] = int(row[IHB_OFFSET])
    measurement = "{}{}".format(INFLUXDB_MEASUREMENT, row[USER_OFFSET].lower())
    timestamp = row[TIMESTAMP_OFFSET]
    logger.info(f"DB WRITE: Time: {timestamp}, Measurement: {measurement}, Fields: {ifx_fields}")
    influx_client.write_points([{
                        "measurement": measurement,
                        "time": timestamp,
                        "fields": ifx_fields
                    }], database=INFLUXDB_NAME)