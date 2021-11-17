using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class LedgerRAM2DataTablesetting
    {       
        public int rowThread = 100;     
    }

    public class LedgerRAM2DataTabledataFlow
    {
        public DataTable LedgerRAM2DataTable(LedgerRAM currentTable, LedgerRAM2DataTablesetting currentSetting)
        {
          
            for (int x = 0; x < currentTable.factTable.Count; x++)
                currentTable.factTable[x][0] = x;           

            ConcurrentDictionary<int, DataTable> dtMultithread = new ConcurrentDictionary<int, DataTable>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            DataTable currentOutput = new DataTable();            

            List<int> rowSegment = new List<int>();
            StringBuilder dataTableString = new StringBuilder();
            List<string> dataTableColumnName = new List<string>();

            rowSegment.Add(1);
            if (currentTable.factTable[0].Count > 1000)
            {
                int rowSegmentLength = Convert.ToInt32(Math.Round((double)((currentTable.factTable[0].Count - 1) / currentSetting.rowThread), 0));

                for (int y = 1; y < currentSetting.rowThread; y++)
                    rowSegment.Add(rowSegmentLength * y);

                rowSegment.Add(currentTable.factTable[0].Count);
            }
            else
            {
                rowSegment.Add(currentTable.factTable[0].Count);
            }          

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new LedgerRAM2CSVdataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };            

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {              
                dtMultithread[currentSegment] = LedgerRAM2DataTableSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, dataTableColumnName, currentTable, currentSetting);               
            });         

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
           

            DataTable dt = new DataTable();

            for (int x = 0; x < currentTable.factTable.Count; x++)
            {
                if (currentTable.dataType[x] == "Number")
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(double));

                else if (currentTable.dataType[x] == "Date")
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(DateTime));

                else
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(string));
            }

            for (int i = 0; i < rowSegment.Count - 1; i++)
               dt.Merge(dtMultithread[i]);

            currentOutput = dt;

            return currentOutput;
        }

        public DataTable LedgerRAM2DataTableSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> dataTableColumnName, LedgerRAM currentTable, LedgerRAM2DataTablesetting currentSetting)
        {  
            int maxColumn = currentTable.factTable.Count;
            DataTable dt = new DataTable();

            for (int x = 0; x < currentTable.factTable.Count; x++)
            {
                if (currentTable.dataType[Convert.ToInt32(currentTable.factTable[x][0])] == "Number")
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(double));

                else if (currentTable.dataType[x] == "Date")
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(DateTime));

                else
                    dt.Columns.Add(currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])], typeof(string));
            }            

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                DataRow dr = dt.NewRow();
                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentTable.dataType[x] == "Text")
                        dr[x] = currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]];

                    else if (currentTable.dataType[x] == "Number")
                        dr[x] = currentTable.factTable[x][y];

                    else if (currentTable.dataType[x] == "Date")                    
                        dr[x] = DateTime.FromOADate(Convert.ToDouble(currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]]));

                    else
                        dr[x] = currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]];                  
                }
                dt.Rows.Add(dr);
            }         

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return dt;
        }
    }
}
