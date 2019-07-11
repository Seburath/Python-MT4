import struct
import pandas as pd
import pymongo
from pymongo import MongoClient
import datetime
import time
from os import listdir
from os.path import isfile, join
import ciso8601

head_s = 148
struct_size = 60

#script body
if __name__ == "__main__":

    path_to_history = "C:\Users\USERNAME\AppData\Roaming\MetaQuotes\Terminal\88A7C6C356B9D73AC70BD2040F0D9829\history\Ava-Real 1\\"
    
    filenames = [f for f in listdir(path_to_history) if isfile(join(path_to_history, f))]
    
    client = MongoClient()
    
    db = client['FIN_DATA']
    
    #do it for all files
    for filename in filenames:
        try:    
            read = 0
            openTime = []
            openPrice = []
            lowPrice = []
            highPrice = []
            closePrice = []
            volume = []
            
            with open(path_to_history+filename, 'rb') as f:
                while True:
                    if read >= head_s:
                        buf = f.read(struct_size)
                        read += struct_size
                        if not buf:
                            break
                        
                        bar = struct.unpack("<Qddddqiq", buf)
                        openTime.append(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bar[0])))
                        openPrice.append(bar[1])
                        highPrice.append(bar[2])
                        lowPrice.append(bar[3])
                        closePrice.append(bar[4])
                        volume.append(bar[5])                  
                    else:           
                        buf = f.read(head_s)
                        read += head_s
                
            data = {'0_openTime':openTime, '1_open':openPrice,'2_high':highPrice,'3_low':lowPrice,'4_close':closePrice,'5_volume':volume}
     
            result = pd.DataFrame.from_dict(data)
            result = result.set_index('0_openTime')
            result.index.name = "DATE_TIME"
            result.columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            
            print "-------------------------------------------------------"
            tableName = filename[:-4]
            print tableName
            print "Data rows for "+tableName+": %s" %len(result)
            
            rows = db[tableName]
            
            #last N rows from table
            if db[tableName].count() > 0:
                #last date in collection
                last_col_date = list(db[tableName].find().skip(db[tableName].count() - 1))
            else:
                last_col_date = 0
            
            #if data exists
            if db[tableName].count() > 0:
                print "last date in collection:"
                dbdte_str = last_col_date[0]['DATE_TIME']
                t = ciso8601.parse_datetime(dbdte_str)
                dbdte = time.mktime(t.timetuple())
                print dbdte_str
                #----------------------------------------------------------------
                
                print "Last date in file:"
                fldte_str = list(result.tail(1).index)[0]
                fldte_str1 = list(result.iloc[-2:].index)[0]
                t = ciso8601.parse_datetime(fldte_str)
                t1 = ciso8601.parse_datetime(fldte_str1)
                fldte = time.mktime(t.timetuple())
                fldte1 = time.mktime(t1.timetuple())
                print fldte_str
                #----------------------------------------------------------------
                
                #get estimate of how many bars are missing
                period = fldte - fldte1
                print "Period %s" %period
                dif = int((fldte - dbdte)/period)
                print "Difference %s" %dif
                d = result.tail(dif)
                
                #if len(lst) == 0:
                for b in range(0, len(d)-1):
                    row = {"DATE_TIME": d.ix[b].name,
                            "OPEN": d.ix[b].OPEN,
                            "HIGH": d.ix[b].HIGH,
                            "LOW": d.ix[b].LOW,
                            "CLOSE": d.ix[b].CLOSE,
                            "VOLUME": d.ix[b].VOLUME}
                        
                    print row
                        
                    try:
                        rows.insert_one(row)
                        print "Passed"
                        if last_col_date == 0:
                            #index it for future use
                            db[tableName].create_index([('DATE_TIME', pymongo.ASCENDING)], unique=True)
                        
                    except Exception as e:
                        print e
                        continue
            else:
                #if len(lst) == 0:
                for b in range(0, len(result)-1):
                    row = {"DATE_TIME": result.ix[b].name,
                            "OPEN": result.ix[b].OPEN,
                            "HIGH": result.ix[b].HIGH,
                            "LOW": result.ix[b].LOW,
                            "CLOSE": result.ix[b].CLOSE,
                            "VOLUME": result.ix[b].VOLUME}
                    
                    try:
                        rows.insert_one(row)
                        if last_col_date == 0:
                            #index it for future use
                            db[tableName].create_index([('DATE_TIME', pymongo.ASCENDING)], unique=True)
                    except Exception as e:
                        print e
                        continue
        except Exception as e:
            print e
            continue   
    
print "\nAll done" 
