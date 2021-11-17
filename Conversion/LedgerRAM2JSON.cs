using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class LedgerRAM2JSONsetting
    {
        public int rowThread = 100; 
        public string tableName { get; set; }
    }

    public class LedgerRAM2JSONoutput
    {
        public StringBuilder jsonString { get; set; }
    }

    public class LedgerRAM2JSONdataFlow
    {
        public StringBuilder LedgerRAM2JSON(LedgerRAM currentTable, LedgerRAM2JSONsetting currentSetting)
        {
            for (int x = 0; x < currentTable.factTable.Count; x++)
                currentTable.factTable[x][0] = x;

            ConcurrentDictionary<int, StringBuilder> jsonStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder jsonString = new StringBuilder();            
            jsonString.Append("{" + Environment.NewLine);
            jsonString.Append("    \"" + currentSetting.tableName + "\": [" + Environment.NewLine);

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
               jsonStringMultithread[currentSegment] = LedgerRAM2JSONsegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                jsonString.Append(jsonStringMultithread[i]);

            jsonString.Append("  ]" + Environment.NewLine);
            jsonString.Append("}" + Environment.NewLine);                           
           
            return jsonString;
        }

        public StringBuilder LedgerRAM2JSONsegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, LedgerRAM2JSONsetting currentSetting)
        {
            StringBuilder jsonString = new StringBuilder();
            int maxRow = currentTable.factTable[0].Count;
            int maxColumn = currentTable.factTable.Count;           

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                jsonString.Append("    {" + Environment.NewLine);
                jsonString.Append("     \"id\": " + y + "," + Environment.NewLine);

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentTable.dataType[x] == "Number")
                    {
                        if (x != maxColumn - 1)
                            jsonString.Append("     \"" + currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])] + "\": " + currentTable.factTable[x][y] + "," + Environment.NewLine);
                        else
                            jsonString.Append("     \"" + currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])] + "\": " + currentTable.factTable[x][y] + Environment.NewLine);
                    }
                    else
                    {
                        if (x != maxColumn - 1)
                            jsonString.Append("     \"" + currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])] + "\": " + "\"" + currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]] + "\"," + Environment.NewLine);
                        else
                            jsonString.Append("     \"" + currentTable.columnName[Convert.ToInt32(currentTable.factTable[x][0])] + "\": " + "\"" + currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]] + "\"" + Environment.NewLine);
                    }
                }
                if (y != maxRow - 1)
                    jsonString.Append("    }," + Environment.NewLine);
                else
                    jsonString.Append("    }" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return jsonString;
        }
    }
}
