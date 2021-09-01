# importing required libraries for model

import os
import sys
import fileinput
import mysql.connector
import random
import time
from kafka import KafkaProducer
import pandas as pd
import json
import shutil



# parameters required to generate json file
inFileName = "/home/dev1-user/RR/json_Auto_gen/tempJson/RC_template.json"
outFileName = "RC_108.py" #default
keyStart = "<P#"
keyEnd = "#P>"
paramValue = ''
inFile = None
outFile = None




# Method to generate the json file./ For this method, We need to enter how many files(n) we want and what is the file generation frequency(t1).
# enter key as 'gjf' to execute
def gen_jsonfiles(n,t1):
    for i in range(n):
        try:
            with open(inFileName, 'r') as inFile:
            #with open('inFileName', 'r+') as inFile:
            #open('outFileName', 'w') as outFile:
                #for line in fileinput.input( inFile ):
                for line in inFile:
                    #print ("line:",line)
                    if line.find(keyStart) == -1:
                        #print("paramKey not found")
                        # write the string to output file and continue
                        outFile.write(line)

                        continue
                    # Found the keyStart in the line. Confirm it is the paramKey
                    print ("line:",line)
                    print("keyStart found")
                    paramKey = line.split(keyStart)[len(line.split(keyStart)) -1 ].split(keyEnd)[0]
                    print ("SPLIT> paramKey:", paramKey)

                    w = paramKey.split(",")
                    print ("w:", w)
                    if w[0] == 'random_file_name' :
                        randm = random.randrange(1000, 10000, 1)
                        outFileName = w[1] + str(randm) +".json"
                        print("outFileName: ", outFileName)
                        outFile = open(outFileName, 'w')
                        # This is filename declaration. Skip writting to output file
                        continue # Skip writting to output file
                    if w[0] == 'write_file_name' :
                        print("Add outFileName in json: ", outFileName)
                        paramValue = outFileName
                    if w[0] == 'set_order_line_prefix' :
                        randm = random.randrange(100, 1000, 1)
                        order_line_prefix = w[1] + str(randm)
                        continue # Skip writting to output file
                    if w[0] == 'fill_order_line_prefix' :
                        paramValue = order_line_prefix

                    if paramKey is not None:
                        if paramValue is None:
                            print("ERROR: *** Key found in file but value not in database ***", paramKey)
                    print("param: " + paramKey + paramValue)
                    if paramKey is not None:
                        # It is a paramKey so get the paramValue
                        print("Found key", paramKey)
                        #paramValue = search_key_value(paramKey)
                        print("key & value", paramKey, paramValue)
                        if paramValue is not None:
                            print("Replace:" + keyStart+paramKey+keyEnd)
                            lineModified = line.replace(keyStart+paramKey+keyEnd, paramValue)
                            print ("lineModified:" + lineModified)
                            outFile.write(lineModified)
                        else:
                            # paramKey found but not paramValue.
                            # It is some other string (not paramKey) and so simply write
                            outFile.write(line)


                    #else:
                    #    outFile.write(line)
                inFile.close()
                time.sleep(t1)
        except IOError as e:
            if inFile is None: print ('Error in opening',inFileName)
            if outFile is None: print ('Error in opening',outFileName)
            print ("Operation failed: %s" % e.strerror)
    path_to_json = "."
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    print(json_files)  # for me this prints ['foo.json']

    #files = ['file1.txt', 'file2.txt', 'file3.txt']
    for f in json_files:
        shutil.copy(f, '/home/dev1-user/RR/ResultofGen_json')




# sending json file to the kafka topic --producture
# enter key as 'prm' to execute this method. i.e to send produced all json to the kafka message.

def kafka_producer(tn):
    topicName = tn
    bootstrap_servers = ['localhost:9092']
    topicName = 'test'
    # producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#for finding the json file in the folder/directorry
    path_to_json = '/home/dev1-user/RR/ResultofGen_json'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    print(json_files)

    from pathlib import Path

    data_folder = Path("/home/dev1-user/RR/ResultofGen_json")

    for f in json_files:
        jsfn = f[:-5]
        f1 = open(f, )
        print(f1)
        data = json.load(f1)
        #ack = producer.send('test', data)
        ack = producer.send(topicName, data)
        
        
        dir = "/home/dev1-user/RR/ResultofGen_json/history_holder"
        # Writing to sample.json
        cdt = str(datetime.datetime.now().strftime("_%b_%d_T%H_%M_%S"))
        with open("/home/dev1-user/RR/ResultofGen_json/history_holder"+jsfn+cdt+".json" , "w") as outfile:
            json.dump(data, outfile)
        
        
        metadata = ack.get()
        print(metadata.topic)
        print(metadata.partition)



inp = input('want to start the process')
while inp != 'q':
    X = input("Please enter your option:\n").split()

    if X[0] == 'g':

        """x = int(input('enter number of file wants to genrate'))
        t = int(input('enter a time in sec'))"""
        print(len(X))
        if len(X) == 3:
            inp, x, t = X
            n = int(x)
            t1 = int(t)
        else:
            inp = X[0]
            n = 2
            t1 = 2
        gen_jsonfiles(n, t1)
        
     
    elif X[0] == 'u':
        topic_nm = str(input('Please enter the topic name') or "testjs")
        kafka_producer(topic_nm)
        print('all messages are produced, process is done')
    #####################remove all files from the location#######################
        mydir = '/home/dev1-user/RR/json_Auto_gen'
        filelist = [f for f in os.listdir(mydir) if f.endswith(".json")]
        for f in filelist:
            os.remove(os.path.join(mydir, f))

        mydir2 = '/home/dev1-user/RR/ResultofGen_json'
        filelist2 = [f2 for f2 in os.listdir(mydir2) if f2.endswith(".json")]
        for f2 in filelist2:
            os.remove(os.path.join(mydir2, f2))
    
    elif X[0] == 'l':
        mydir2 = '/home/dev1-user/RR/ResultofGen_json'
        filelist2 = [f2 for f2 in os.listdir(mydir2) if f2.endswith(".json")]
        if len(filelist2):
            print("There are %s files, ready to process:: %s" % (len(filelist2), filelist2))
        else:
            print('No JSON file to process/send to kafka')
    else:
        inp = X[0]

   
# make simple menu gjf-1,prm -2...... 

#value_serializer
#store the json file -which are gen