#!/bin/bash
# Generate bulk insert commands based on folder contents

# printf "WAITFOR DELAY '03:00'\nPRINT 'Done waiting'\nPRINT SYSDATETIME()\n" > bulk_insert.sql
# echo *.csv* | xargs -n1 echo | sed -r 's/\.gz//' | sort | uniq | xargs ./gen_sql.sh >> bulk_insert.sql
BASE="D:\\data\\temp_niko\\taxi-rides\\"

while (( "$#" )); do
  F="$1"
  echo "BULK INSERT [Niko_test].[dbo].[taxi_trips] FROM '$BASE$F' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ERRORFILE = '$BASE$F.errors', ROWTERMINATOR = '\\n', MAXERRORS = 1000000, TABLOCK)"
  echo GO
  echo "PRINT 'Done with $F'"
  echo 'PRINT SYSDATETIME()'
  echo ''
  shift
done
