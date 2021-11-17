using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class unifyTableSetting
    {
        public int columnThread = 100;       
        public List<string> leftTableColumn { get; set; }
        public List<string> rightTableColumn { get; set; }
        public List<string> commonTableUpperColumnName { get; set; }
        public List<int> commonTableColumnID { get; set; }
        public List<int> revisedRightTableColumnID { get; set; }
    }

    public class unifyTable
    {
        public LedgerRAM unifyTableProcess(LedgerRAM leftTable, LedgerRAM rightTable, unifyTableSetting currentSetting)
        {
            ConcurrentDictionary<int, List<double>> unifyTableFactTable = new ConcurrentDictionary<int, List<double>>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, unifyTable> writeColumnThread = new ConcurrentDictionary<int, unifyTable>();
            Dictionary<int, int> u2x = new Dictionary<int, int>();

            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>(leftTable.key2Value);
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>(leftTable.value2Key);

            int u = 0;          

            for (int x = 0; x < currentSetting.rightTableColumn.Count; x++)
            {
                if (currentSetting.commonTableUpperColumnName.Contains(rightTable.columnName[x].ToUpper()))
                {
                    u2x.Add(u, x);
                    unifyTableFactTable.TryAdd(u, new List<double>());
                    u++;
                }
            }          

            for (int worker = 0; worker < u; worker++)               
                    writeColumnThread.TryAdd(worker, new unifyTable());            

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };           

            Parallel.For(0, u, options, uu =>
            {  
                  unifyTableFactTable[uu] = unifyTableColumn(uu, u2x, checkThreadCompleted, leftTable, rightTable, currentSetting, key2Value, value2Key);
            });

            do
            {
                Thread.Sleep(5);

            } while (checkThreadCompleted.Count < u2x.Count);        

            Dictionary<int, string> rightTableDataType = new Dictionary<int, string>();
            Dictionary<int, string> rightTableColumnName = new Dictionary<int, string>();
            Dictionary<string, int> rightTableUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> rightTableFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> rightTableKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> rightTableValue2Key = new Dictionary<int, Dictionary<string, double>>();
            int unmatchKey = 0;            

            string text = null;
            List<int> rightTableColumnID = new List<int>();

            for (int x = 0; x < currentSetting.commonTableColumnID.Count; x++)
            {
                rightTableColumnID.Add(x);
                rightTableDataType.Add(x, leftTable.dataType[currentSetting.commonTableColumnID[x]]);
                rightTableColumnName.Add(x, leftTable.columnName[currentSetting.commonTableColumnID[x]]);
                rightTableUpperColumnName2ID.Add(leftTable.columnName[currentSetting.commonTableColumnID[x]].ToUpper(), x);

                rightTableFactTable.Add(x, unifyTableFactTable[x]);

                rightTableFactTable[x][0] = x;

                if (leftTable.dataType[currentSetting.commonTableColumnID[x]] != "Number")
                {
                    rightTableKey2Value.Add(x, leftTable.key2Value[currentSetting.commonTableColumnID[x]]);
                    rightTableValue2Key.Add(x, leftTable.value2Key[currentSetting.commonTableColumnID[x]]);
                }
            }

            int commonColumnID = currentSetting.commonTableColumnID.Count;

            u = 0;
            for (int x = 0; x < rightTable.columnName.Count; x++)
            {
                if (!currentSetting.commonTableUpperColumnName.Contains(rightTable.columnName[x].ToUpper()))
                {
                    rightTableDataType.Add(u + commonColumnID, rightTable.dataType[x]);
                    rightTableColumnName.Add(u + commonColumnID, rightTable.columnName[x]);
                    rightTableUpperColumnName2ID.Add(rightTable.columnName[x].ToUpper(), u + commonColumnID);
                    rightTableFactTable.Add(u + commonColumnID, rightTable.factTable[x]);

                    rightTableFactTable[u + commonColumnID][0] = u + commonColumnID;

                    if (rightTable.dataType[x] != "Number")
                    {
                        rightTableKey2Value.Add(u + commonColumnID, rightTable.key2Value[x]);
                        rightTableValue2Key.Add(u + commonColumnID, rightTable.value2Key[x]);
                    }

                    if (rightTable.dataType[x] == "Number")
                        rightTableFactTable[u + commonColumnID].Add(0);
                    else
                    {
                        if (rightTable.dataType[x] == "Text")
                            text = "Unmatch";

                        if (rightTable.dataType[x] == "Date")
                            text = "0";

                        if (rightTable.dataType[x] == "Period")
                            text = "1900p01";

                        if (rightTableValue2Key[u + commonColumnID].ContainsKey(text)) // same master record
                            rightTableFactTable[u + commonColumnID].Add(rightTableValue2Key[x][text]);

                        else // add new master record
                        {
                            unmatchKey = rightTableValue2Key[x].Count;
                            rightTableKey2Value[u + commonColumnID].Add(unmatchKey, text);
                            rightTableValue2Key[u + commonColumnID].Add(text, unmatchKey);
                            rightTableFactTable[u + commonColumnID].Add(unmatchKey);
                        }
                    }
                    u++;
                }
            }

            rightTable.dataType = rightTableDataType;
            rightTable.columnName = rightTableColumnName;
            rightTable.upperColumnName2ID = rightTableUpperColumnName2ID;
            rightTable.factTable = rightTableFactTable;
            rightTable.key2Value = rightTableKey2Value;
            rightTable.value2Key = rightTableValue2Key;

            return rightTable;
        }
        public List<double> unifyTableColumn(int u, Dictionary<int, int> u2x, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM leftTable, LedgerRAM rightTable, unifyTableSetting currentSetting, Dictionary<int, Dictionary<double, string>> key2Value, Dictionary<int, Dictionary<string, double>> value2Key)
        {
            List<double> unifyTableFactTable = new List<double>();
            unifyTableFactTable.Add(u);
            int x = u2x[u];
            int count;
          
            var col = currentSetting.commonTableColumnID[u];

            for (int y = 1; y < rightTable.factTable[0].Count; y++)
            {
                var text = rightTable.key2Value[x][rightTable.factTable[x][y]];

                if (!leftTable.value2Key[col].ContainsKey(text) && !value2Key[col].ContainsKey(text))
                {
                    count = value2Key[col].Count;
                    key2Value[col].Add(count, text);
                    value2Key[col].Add(text, count);
                    unifyTableFactTable.Add(value2Key[col][text]);                  
                }
                else                 
                    unifyTableFactTable.Add(value2Key[col][text]);               
            }

            leftTable.key2Value[col] = key2Value[col];
            leftTable.value2Key[col] = value2Key[col];

            checkThreadCompleted.Enqueue(u);
            return unifyTableFactTable;

        }
    }
}