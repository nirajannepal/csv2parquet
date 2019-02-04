import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

csvFile = "C:/Users/nirajan.nepal/Desktop/talend/db/airlines/output.csv";
csvOutputFile = "C:/Users/nirajan.nepal/Desktop/talend/db/airlines/forarc/output.parquet";

start_time = time.time();
dataFrame = pd.read_csv(
    filepath_or_buffer=csvFile,
    delimiter=","
);

#writing in parquet format
table = pa.Table.from_pandas(dataFrame)
print("Parquet",table)
#parquet table name is example.parquet
pq.write_table(table,csvOutputFile)

#reading single parquet file. Read a single file back with read_table:
table2 = pq.read_table(csvOutputFile)

print("Parquet",table2.to_pandas())

print("----%.2f-Sec----" % (time.time() - start_time))