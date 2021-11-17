using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class LedgerRAM2HTMLsetting
    {
        public int rowThread = 100;
    }

    public class LedgerRAM2HTMLdataFlow
    {
        public StringBuilder LedgerRAM2HTML(LedgerRAM currentTable, LedgerRAM2HTMLsetting currentSetting)
        {
            for (int x = 0; x < currentTable.factTable.Count; x++)
                currentTable.factTable[x][0] = x;

            ConcurrentDictionary<int, StringBuilder> htmlStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder htmlString = new StringBuilder();
            List<string> htmlColumnName = new List<string>();

            Dictionary<int, string> columnName = currentTable.cutColumnNamePrefix(currentTable);

            htmlString.Append("<table class=\"table\">" + Environment.NewLine);
            htmlString.Append("    <thead>" + Environment.NewLine);
            htmlString.Append("      <tr>" + Environment.NewLine);

            if (currentTable.crosstabHeader != null)
            {
                for (int i = 0; i < currentTable.crosstabHeader.Count; i++)
                {
                    for (int j = 0; j < currentTable.crosstabHeader[i].Count; j++)                   
                        htmlString.Append("        <th>" + currentTable.crosstabHeader[i][j] + "</th>" + Environment.NewLine);
                    
                    htmlString.Append("      </tr>" + Environment.NewLine);
                }
            }

            for (int x = 0; x < currentTable.factTable.Count; x++)            
                htmlString.Append("        <th>" + columnName[Convert.ToInt32(currentTable.factTable[x][0])] + "</th>" + Environment.NewLine);            

            htmlString.Append("      </tr>" + Environment.NewLine);
            htmlString.Append("    </thead>" + Environment.NewLine);
            htmlString.Append("    <tbody>" + Environment.NewLine);

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
                htmlStringMultithread[currentSegment] = LedgerRAM2HTMLsegment(rowSegment, currentSegment, checkSegmentThreadCompleted, htmlColumnName, currentTable, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                htmlString.Append(htmlStringMultithread[i]);
           
            htmlString.Append("    </tbody>" + Environment.NewLine);
            htmlString.Append("</table>" + Environment.NewLine);           
            
            return htmlString;
        }

        public StringBuilder LedgerRAM2HTMLsegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> htmlColumnName, LedgerRAM currentTable, LedgerRAM2HTMLsetting currentSetting)
        {
            StringBuilder htmlString = new StringBuilder();           
            int maxColumn = currentTable.factTable.Count;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                htmlString.Append("        <tr>" + Environment.NewLine);               

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentTable.dataType[x] == "Number")
                        htmlString.Append("          <td>" + currentTable.factTable[x][y] + "</td>" + Environment.NewLine);                   
                    else
                        htmlString.Append("          <td>" + currentTable.key2Value[Convert.ToInt32(currentTable.factTable[x][0])][currentTable.factTable[x][y]] + "</td>" + Environment.NewLine);                    
                }
                htmlString.Append("        </tr>" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return htmlString;
        }
    }
}
