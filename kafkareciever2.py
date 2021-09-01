from kafka import KafkaConsumer
import json
import sys
import snowflake.connector


ctx = snowflake.connector.connect(
    user='AnoopSR',
    password='Anoop@8197',
    account='ev02808.us-central1.gcp',
    warehouse='DEMO_WH',
    database='k_to_sf',
    schema='public')
cs = ctx.cursor()
cs.execute(f"select count(v) from k_to_sf.public.v1")
myresult = cs.fetchall()
# 4983 records were unwanted in my kafka so, sending records from 4983

count = (myresult[0][0])+4983


bootstrap_servers = ['localhost:9092']
topicName = 'testjs'
consumer = KafkaConsumer(topicName,bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest')


try:
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
        myvar = message.value
        myvar2 = str(myvar)
        # print(myvar)
        # print(myvar2[2:-1])
        x = myvar2[2:-1]
        #print(myvar)
        json_object = json.dumps(myvar.decode("utf-8"))
        # Writing to sample.json
        """with open("sample.json", "w") as outfile:
            outfile.write(str(json_object.encode("utf-8")).replace('\\',''))"""

########loading messages from kafka to snowflake#####################################
        if message.offset > count:
            x = json.loads(x)
            print('yes')
            cs.execute("insert into V1(v) (select PARSE_JSON('" + json.dumps(x) + "'))").fetchone()
except KeyboardInterrupt:
    sys.exit()

#testjs:0:4996: