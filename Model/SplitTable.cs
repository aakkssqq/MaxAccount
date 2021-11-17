using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class splitTableSetting
    {        
        public int rowThread = 20;
        public int columnThread = 20;
        public List<string> selectColumn { get; set; }
        public Dictionary<int, int> unmatchTableDict { get; set; }
    }

    public class splitTable
    {
        public Dictionary<string, LedgerRAM> splitTableByKey(LedgerRAM leftTable, LedgerRAM rightTable, splitTableSetting currentSetting)
        {
            ConcurrentDictionary<int, List<double>> matchFactTable = new ConcurrentDictionary<int, List<double>>();
            Dictionary<int, string> matchDataType = new Dictionary<int, string>();
            Dictionary<int, string> matchColumnName = new Dictionary<int, string>();
            Dictionary<string, int> matchUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, Dictionary<double, string>> matchKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> matchValue2Key = new Dictionary<int, Dictionary<string, double>>();

            ConcurrentDictionary<int, List<double>> unmatchFactTable = new ConcurrentDictionary<int, List<double>>();
            Dictionary<int, string> unmatchDataType = new Dictionary<int, string>();
            Dictionary<int, string> unmatchColumnName = new Dictionary<int, string>();
            Dictionary<string, int> unmatchUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, Dictionary<double, string>> unmatchKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> unmatchValue2Key = new Dictionary<int, Dictionary<string, double>>();
            
            for (int x = 0; x < leftTable.dataType.Count; x++)
            {               
                matchFactTable.TryAdd(x, new List<double>());
                unmatchFactTable.TryAdd(x, new List<double>());
                matchColumnName.Add(x, leftTable.columnName[x]);
                unmatchColumnName.Add(x, leftTable.columnName[x]);
                matchUpperColumnName2ID.Add(leftTable.columnName[x].ToUpper(), x);
                unmatchUpperColumnName2ID.Add(leftTable.columnName[x].ToUpper(), x);
                matchDataType.Add(x, leftTable.dataType[x]);
                unmatchDataType.Add(x, leftTable.dataType[x]);

                if (leftTable.dataType[x] != "Number")
                {
                    matchKey2Value.Add(x, leftTable.key2Value[x]);
                    unmatchKey2Value.Add(x, leftTable.key2Value[x]);
                    matchValue2Key.Add(x, leftTable.value2Key[x]);
                    unmatchValue2Key.Add(x, leftTable.value2Key[x]);
                }
            }

            ConcurrentDictionary<int, splitTable> writeColumnThread = new ConcurrentDictionary<int, splitTable>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();

            for (int worker = 0; worker < leftTable.dataType.Count; worker++)
                writeColumnThread.TryAdd(worker, new splitTable());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, leftTable.dataType.Count, options, x =>
            {
                //(matchFactTable[x], unmatchFactTable[x]) = splitTableSegment(x, checkThreadCompleted, leftTable, rightTable, currentSetting);
               
                matchFactTable[x] = getMatchColumn(x, checkThreadCompleted, leftTable, rightTable, currentSetting);
            });

            do
            {
                Thread.Sleep(5); 
            } while (checkThreadCompleted.Count < leftTable.dataType.Count);

            Dictionary<string, LedgerRAM> splitedTable = new Dictionary<string, LedgerRAM>();

            splitedTable.Add("match", new LedgerRAM());
            splitedTable["match"].columnName = matchColumnName;            
            splitedTable["match"].upperColumnName2ID = matchUpperColumnName2ID;            
            splitedTable["match"].dataType = matchDataType;            
            splitedTable["match"].factTable = new Dictionary<int, List<double>>(matchFactTable);            
            splitedTable["match"].key2Value = matchKey2Value;            
            splitedTable["match"].value2Key = matchValue2Key;

            return splitedTable;
        }
        public (List<double> matchFactTable, List<double> unmatchFactTable) splitTableSegment(int x, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM leftTable, LedgerRAM rightTable, splitTableSetting currentSetting)
        {
            List<double> matchFactTable = new List<double>();
            List<double> unmatchFactTable = new List<double>();

            unmatchFactTable.Add(x);
            matchFactTable.Add(x);

            for (int y = 1; y < leftTable.factTable[0].Count; y++)
                if (currentSetting.unmatchTableDict.ContainsKey(y))
                {
                  //  unmatchFactTable.Add(leftTable.factTable[x][y]);
                }
                else
                    matchFactTable.Add(leftTable.factTable[x][y]);

            checkThreadCompleted.Enqueue(x);
            return (matchFactTable, unmatchFactTable);
        }     
        public List<double> getMatchColumn(int x, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM leftTable, LedgerRAM rightTable, splitTableSetting currentSetting)
        {
            List<double> matchFactTable = new List<double>();

            ConcurrentDictionary<int, List<double>> factTableMultithread = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, splitTable> concurrentRowSegment = new ConcurrentDictionary<int, splitTable>();
            Dictionary<int, ConcurrentQueue<int>> checkSegmentThreadCompleted = new Dictionary<int, ConcurrentQueue<int>>();
            checkSegmentThreadCompleted.Add(x, new ConcurrentQueue<int>());

            List<int> rowSegment = new List<int>();
            
            rowSegment.Add(1);
            if (leftTable.factTable[0].Count > 1000)
            {
                int rowSegmentLength = Convert.ToInt32(Math.Round((double)((leftTable.factTable[0].Count - 1) / currentSetting.rowThread), 0));

                for (int y = 1; y < currentSetting.rowThread; y++)
                    rowSegment.Add(rowSegmentLength * y);

                rowSegment.Add(leftTable.factTable[0].Count);
            }
            else
            {
                rowSegment.Add(leftTable.factTable[0].Count);
            }

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new splitTable());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                factTableMultithread[currentSegment] = getMatchSegment(x, rowSegment, currentSegment, checkSegmentThreadCompleted, leftTable,rightTable, currentSetting);
            });

            do
            {
                Thread.Sleep(5);             
            } while (checkSegmentThreadCompleted[x].Count < rowSegment.Count - 1);

            List<double> resultFactTable = new List<double>();
            
            for (int s = 0; s < rowSegment.Count - 1; s++)
                resultFactTable.AddRange(factTableMultithread[s]);

            checkThreadCompleted.Enqueue(x);
            return resultFactTable;           
        }
        public List<double> getMatchSegment(int x, List<int> rowSegment, int currentSegment, Dictionary<int, ConcurrentQueue<int>> checkSegmentThreadCompleted, LedgerRAM leftTable, LedgerRAM rightTable, splitTableSetting currentSetting)
        {
            List<double> matchFactTable = new List<double>();

            if (currentSegment == 0)
                matchFactTable.Add(x);

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)               
                if (!currentSetting.unmatchTableDict.ContainsKey(y))
                    matchFactTable.Add(leftTable.factTable[x][y]);
          
            checkSegmentThreadCompleted[x].Enqueue(currentSegment);
            return matchFactTable;
        }
    }
}

