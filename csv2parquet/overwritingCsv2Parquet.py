import datetime
import os
import time
from shutil import copy2

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# csvFile = "C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/output.csv";
csvFile = "C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/sample/1989.csv";


def fileExist(fileName):
    if os.path.exists(fileName):
        return True
    return False


def stripCSVExtension(fileName):
    return fileName.replace(".csv", "")


def getParquetOutputFileName(csvFilePath):
    # csvFile = "C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/output.csv";
    csvFile = csvFilePath;

    parquetFileItems = csvFile.split("/");

    # construct output folder
    parquestOutputFolder = "";
    for index in range((len(parquetFileItems) - 1)): # iterate upto C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc
        parquestOutputFolder += parquetFileItems[index] + "/"
    parquestOutputFolder += "output" # creates output file named C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/output

    parquestOutputFile = parquestOutputFolder + "/" + stripCSVExtension(parquetFileItems[-1]) + ".parquet" #adds extension C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/output.parquet
    return parquestOutputFile;


start_time = time.time();

dataFrame = pd.read_table(
    filepath_or_buffer=csvFile,
    delimiter=","
);
# print("dataFrame ", dataFrame);

table = pa.Table.from_pandas(dataFrame);
# print("parquentFormat table ", table);

parquetOutputFilePath = getParquetOutputFileName(csvFile);

if fileExist(parquetOutputFilePath):
    #Creates a hitrory folder and datatime as string and names the new parquet file inside history folder.

    oldParquetFilePath = parquetOutputFilePath;
    print("oldParquetFileName", oldParquetFilePath);

    dateInString = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d-%H-%M-%S"); #creates string with date and time

    newParquestFilePath = "";
    parquetOutputFileNameList = oldParquetFilePath.split("/");
    for index in (range(len(parquetOutputFileNameList) - 1)):
        newParquestFilePath += parquetOutputFileNameList[index] + "/"
    newParquestFilePath += "history"

    newParquestFilePath = newParquestFilePath + "/" + parquetOutputFileNameList[-1].replace(".parquet",
                                                                                            "") + "-" + dateInString + ".parquet"
    print("csvFileName", csvFile,
          "old parquest", oldParquetFilePath,
          "new parquet", newParquestFilePath)

    copy2(oldParquetFilePath, newParquestFilePath) #source,destination

    parquetOutputFilePath = newParquestFilePath;

pq.write_table(table, parquetOutputFilePath);

# readTableFromFile = pq.ParquetFile("someTable")
# print(readTableFromFile.metadata)
# print(readTableFromFile.schema)


readTableFromFile = pq.read_table(parquetOutputFilePath);
print("parquentFormat table ", readTableFromFile);

# to pandas
readDataFrame = readTableFromFile.to_pandas();
print("read the data from file ", readDataFrame);

print("----%.2f-----" % (time.time() - start_time))
