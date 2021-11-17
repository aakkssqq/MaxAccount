using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class LedgerRAM2CSVsetting
    {
        public string separator = ",";
        public int rowThread = 100;
        public List<string> columnName { get; set; }
    }    

    public class LedgerRAM2CSVdataFlow
    {
        public StringBuilder LedgerRAM2CSV(LedgerRAM currentTable, LedgerRAM2CSVsetting currentSetting)
        {
            /*
            for (int x = 0; x < currentTable.columnName.Count; x++)
                Console.WriteLine(currentTable.columnName[x] + "  " + currentTable.dataType[x]);
            */

            for (int x = 0; x < currentTable.factTable.Count; x++)
                currentTable.factTable[x][0] = x;           

            ConcurrentDictionary<int, StringBuilder> csvStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder csvString = new StringBuilder();           
            string separator = currentSetting.separator;
           
            Dictionary<int, string> columnName = currentTable.cutColumnNamePrefix(currentTable);           

            if (currentTable.crosstabHeader != null)
            {
                for (int i = 0; i < currentTable.crosstabHeader.Count; i++)
                {
                    for (int j = 0; j < currentTable.crosstabHeader[i].Count; j++)
                    {
                        if (j > 0) csvString.Append(separator);
                        csvString.Append(currentTable.crosstabHeader[i][j]);                      
                    }
                    csvString.Append(Environment.NewLine);
                }
            }
            
            for (int x = 0; x < currentTable.factTable.Count; x++)
            {
                if (x > 0) csvString.Append(separator);

                if (columnName[Convert.ToInt32(currentTable.factTable[x][0])].Contains(separator))
                    csvString.Append((char)34 + columnName[Convert.ToInt32(currentTable.factTable[x][0])] + (char)34);
                else
                    csvString.Append(columnName[Convert.ToInt32(currentTable.factTable[x][0])]);               
            }

            csvString.Append(Environment.NewLine);

            rowSegment.Add(1);
            if (currentTable.factTable[0].Count > 1000)
            {
                int rowSegmentLength = Convert.ToInt32(Math.Round((double)((currentTable.factTable[0].Count - 1) / currentSetting.rowThread), 0));

                for (int y = 1; y < currentSetting.rowThread; y++)
                    rowSegment.Add(rowSegmentLength * y);

                rowSegment.Add(currentTable.factTable[0].Count);
            }
            else
                rowSegment.Add(currentTable.factTable[0].Count);

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new LedgerRAM2CSVdataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {               
                 csvStringMultithread[currentSegment] = LedgerRAM2CSVsegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting);
            });           

            do
            {
                Thread.Sleep(10);             
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
           
            for (int i = 0; i < rowSegment.Count - 1; i++)
                csvString.Append(csvStringMultithread[i]);
            
            return csvString;
        }
        public StringBuilder LedgerRAM2CSVsegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, LedgerRAM2CSVsetting currentSetting)
        {
            StringBuilder csvString = new StringBuilder();
            string separator = currentSetting.separator;
            string currentCell;           
            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (currentTable.dataType[x] == "Number")
                        currentCell = currentTable.factTable[x][y].ToString();
                    else
                        currentCell = currentTable.key2Value[x][currentTable.factTable[x][y]].ToString();                     

                    if (x > 0) csvString.Append(separator);

                    if (currentCell.Contains(separator))
                    {
                        if (currentCell.Contains("\""))
                            currentCell = currentCell.ToString().Replace("\"", "\"\"");

                        csvString.Append((char)34 + currentCell + (char)34);
                    }
                    else
                    {
                        csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return csvString;
        }
    }
}
