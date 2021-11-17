using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class LedgerRAM2XMLsetting
    {
        public int rowThread = 100;
        public string tableName { get; set; }
    }  

    public class LedgerRAM2XMLdataFlow
    {
        public StringBuilder LedgerRAM2XML(LedgerRAM currentTable, LedgerRAM2XMLsetting currentSetting)
        {
            for (int x = 0; x < currentTable.factTable.Count; x++)
                currentTable.factTable[x][0] = x;

            ConcurrentDictionary<int, StringBuilder> xmlStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder xmlString = new StringBuilder();
            List<string>  xmlColumnName = new List<string>();
            string tempColumnName;

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                tempColumnName = currentTable.columnName[x].Replace(" ", "_x0020_");
                tempColumnName = tempColumnName.Replace("/", "_x002F_");
                xmlColumnName.Add(tempColumnName);
            }

            xmlString.Append("<?xml version=\"1.0\" standalone=\"yes\"?>" + Environment.NewLine);
            xmlString.Append("<NewDataSet>" + Environment.NewLine);

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
                xmlStringMultithread[currentSegment] = LedgerRAM2XMLsegment(rowSegment, currentSegment, checkSegmentThreadCompleted, xmlColumnName, currentTable, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                xmlString.Append(xmlStringMultithread[i]);
            
            xmlString.Append("</NewDataSet>" + Environment.NewLine);           
           
            return xmlString;
        }

        public StringBuilder LedgerRAM2XMLsegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> xmlColumnName, LedgerRAM currentTable, LedgerRAM2XMLsetting currentSetting)
        {
            StringBuilder xmlString = new StringBuilder();
            int maxRow = currentTable.factTable[0].Count;
            int maxColumn = currentTable.factTable.Count;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                xmlString.Append("   <" + currentSetting.tableName + ">" + Environment.NewLine);                

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentTable.dataType[x] == "Number")
                        xmlString.Append("     <" + xmlColumnName[Convert.ToInt32(currentTable.factTable[x][0])] + ">" + currentTable.factTable[x][y] + "</" + xmlColumnName[x] + ">"  + Environment.NewLine);                                         
                    else
                        xmlString.Append("     <" + xmlColumnName[Convert.ToInt32(currentTable.factTable[x][0])] + ">" + currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]] + "</" + xmlColumnName[Convert.ToInt32(currentTable.factTable[x][0])] + ">" + Environment.NewLine);
                }
                xmlString.Append("   </" + currentSetting.tableName + ">" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return xmlString;
        }
    }
}
