using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.IO;

namespace MaxAccount
{
    public class conditionalJoinSetting
    {
        public int columnThread = 100;
        public string leftTable { get; set; }
        public string rightTable { get; set; }
        public List<string> leftTableColumn { get; set; }
        public List<string> rightTableColumn { get; set; }
        public string joinTableType { get; set; }       
    }

    public class conditionalJoin
    {
        public LedgerRAM conditionalJoinProcess(LedgerRAM leftTable, LedgerRAM rightTable, conditionalJoinSetting currentSetting)
        {
            if (currentSetting.rightTableColumn[0].ToUpper() == "EFFECTIVE DATE")
            {  
                List<string> dateColumnName = new List<string>();
                dateColumnName.Add(rightTable.columnName[0]);

                date2EffectiveDate newDate2EffectiveDate = new date2EffectiveDate();
                date2EffectiveDateSetting setDate2EffectiveDate = new date2EffectiveDateSetting();
                setDate2EffectiveDate.dateColumnName = dateColumnName;

                rightTable = newDate2EffectiveDate.date2EffectiveDateProcess(rightTable, setDate2EffectiveDate);

                /*
                LedgerRAM currentProcess = new LedgerRAM();
                LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                StringBuilder _LEDGERRAM2CSV = currentProcess.LedgerRAM2CSV(rightTable, setLEDGERRAM2CSV);

                using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + "aaa.csv"))
                {
                    toDisk.Write(_LEDGERRAM2CSV);
                    toDisk.Close();
                }
                */
            }


            Dictionary<int, string> revisedColumnName = new Dictionary<int, string>();
            Dictionary<string, int> revisedUpperColumnName2ID = new Dictionary<string, int>();

            for (int x = 0; x < rightTable.columnName.Count; x++)
            {
                if (currentSetting.rightTableColumn.Contains(rightTable.columnName[x]))
                {
                    revisedColumnName.Add(x, currentSetting.leftTableColumn[x]);
                    revisedUpperColumnName2ID.Add(currentSetting.leftTableColumn[x].ToUpper(), x);
                }
                else
                {
                    revisedColumnName.Add(x, rightTable.columnName[x]);
                    revisedUpperColumnName2ID.Add(rightTable.columnName[x].ToUpper(), x);
                }
            }

            rightTable.columnName = revisedColumnName; // sync column name of rightTable with leftTable
            rightTable.upperColumnName2ID = revisedUpperColumnName2ID;


            List<string> sameColumn = new List<string>();

            for (int x = 0; x < currentSetting.leftTableColumn.Count; x++)
                sameColumn.Add(currentSetting.leftTableColumn[x]);

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            List<int> commonTableColumnID = new List<int>();
            List<int> resultConditionalJoinColumnID = new List<int>();
            List<string> commonTableUpperColumnName = new List<string>();
            List<string> commonTableColumnName = new List<string>();

            foreach (var pair in leftTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            for (int x = 0; x < sameColumn.Count; x++)
            {
                if (upperColumnName2ID.ContainsKey(sameColumn[x].ToUpper()))
                {
                    commonTableColumnID.Add(upperColumnName2ID[sameColumn[x].ToUpper()]);
                    commonTableColumnName.Add(sameColumn[x]);
                    commonTableUpperColumnName.Add(sameColumn[x].ToUpper());
                }
                else
                    resultConditionalJoinColumnID.Add(x);
            }

            for (int x = 0; x < rightTable.columnName.Count; x++)
            {
                if (!upperColumnName2ID.ContainsKey(rightTable.columnName[x].ToUpper()))
                    resultConditionalJoinColumnID.Add(x);
            }

            List<int> revisedRightTableColumnID = new List<int>();
            for (int x = 0; x < commonTableColumnID.Count; x++)
                revisedRightTableColumnID.Add(x);

            Dictionary<string, string> residualUpperColumnName2Value = new Dictionary<string, string>();

            for (int x = 0; x < rightTable.factTable.Count; x++) // last row of conditionalJoin
            {
                if (rightTable.dataType[x] != "Number")
                    residualUpperColumnName2Value.Add(rightTable.columnName[x].ToUpper(), rightTable.key2Value[x][rightTable.factTable[x][rightTable.factTable[0].Count - 1]]);
                else
                    residualUpperColumnName2Value.Add(rightTable.columnName[x].ToUpper(), rightTable.factTable[x][rightTable.factTable[0].Count - 1].ToString());
            }
         
            selectColumn newSelectColumn = new selectColumn();
            selectColumnSetting setSelectColumn = new selectColumnSetting();
            setSelectColumn.selectColumn = commonTableColumnName;

            /*
            Console.WriteLine();

            foreach (var pair in rightTable.columnName)
                Console.WriteLine("currentTable.columnName " + pair.Value);

            foreach (var pair in rightTable.upperColumnName2ID)
                Console.WriteLine("currentTable.upperColumnName2ID " + pair.Key);
            */
            LedgerRAM newTableBySelectColumn = newSelectColumn.selectColumnName(rightTable, setSelectColumn);
            filterSetting setFilter = new filterSetting();
            setFilter.filterType = "And";

            LedgerRAM currentOutput = conditionalJoinByConditionList1(leftTable, rightTable, newTableBySelectColumn, setFilter, resultConditionalJoinColumnID, currentSetting);

            return currentOutput;
        }

        public LedgerRAM conditionalJoinByConditionList1(LedgerRAM currentTable, LedgerRAM conditionalJoinList, LedgerRAM filterList, filterSetting currentSetting, List<int> resultConditionalJoinColumnID, conditionalJoinSetting joinCurrentSetting)
        {           
            Dictionary<string, string> compareOperatorDict = new Dictionary<string, string>();
            compareOperatorDict.Add(">=", "Greater Than or Equal");
            compareOperatorDict.Add(">", "Greater Than");
            compareOperatorDict.Add("<=", "Less Than or Equal");
            compareOperatorDict.Add("<", "Less Than");
            compareOperatorDict.Add("!=", "Not Equal");
            compareOperatorDict.Add("=", "Equal");
            compareOperatorDict.Add("..", "Range");

            Dictionary<string, string> reserveSpecialCharDict = new Dictionary<string, string>();
            reserveSpecialCharDict.Add(">", null);
            reserveSpecialCharDict.Add("<", null);
            reserveSpecialCharDict.Add("=", null);
            reserveSpecialCharDict.Add("!", null);
            reserveSpecialCharDict.Add(".", null);
            reserveSpecialCharDict.Add(",", null);
            reserveSpecialCharDict.Add("\"", null);

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<int> selectedColumnID = new List<int>();

            for (int x = 0; x < filterList.columnName.Count; x++)
                selectedColumnID.Add(upperColumnName2ID[filterList.columnName[x].ToUpper()]);

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM> tableAfterConditionalJoin = new ConcurrentDictionary<int, LedgerRAM>();
            
            for (int y = 1; y < filterList.factTable[0].Count; y++)
                tableAfterConditionalJoin.TryAdd(y, new LedgerRAM());
        
            computeTextColumn newTextColumn = new computeTextColumn();
            computeColumn newColumn = new computeColumn();
            computeTextColumnSetting setComputeTextColumn = new computeTextColumnSetting();
            computeColumnSetting setComputeColumn = new computeColumnSetting();
            List<string> refColumnName = new List<string>();
            Dictionary<int, int> _matchedRow = new Dictionary<int, int>();
            filter newFilter = new filter();

            Dictionary<int, Dictionary<double, string>> tempKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> tempValue2Key = new Dictionary<int, Dictionary<string, double>>();

            for (int x = 0; x < resultConditionalJoinColumnID.Count; x++)
            {
                if (conditionalJoinList.dataType[resultConditionalJoinColumnID[x]] != "Number")
                {
                    tempKey2Value.Add(x, new Dictionary<double, string>());
                    tempValue2Key.Add(x, new Dictionary<string, double>());
                }
            }

            int conditionalJoinRowCount = filterList.factTable[0].Count;

            if (joinCurrentSetting.joinTableType.ToUpper() == "RESIDUALJOIN")
            {
                for (int y = 1; y < conditionalJoinRowCount; y++)
                {
                    tableAfterConditionalJoin[y] = newFilter.filterByOneRowConditionList(y, _matchedRow, checkThreadCompleted, currentTable, filterList, currentSetting, selectedColumnID, compareOperatorDict, reserveSpecialCharDict);

                    foreach (var pair in tableAfterConditionalJoin[y].matchedRow)
                        _matchedRow.Add(pair.Key, pair.Value);

                    for (int x = 0; x < resultConditionalJoinColumnID.Count; x++)
                    {
                        if (conditionalJoinList.dataType[resultConditionalJoinColumnID[x]] != "Number")
                        {
                            refColumnName.Clear();
                            refColumnName.Add("\"" + conditionalJoinList.key2Value[resultConditionalJoinColumnID[x]][conditionalJoinList.factTable[resultConditionalJoinColumnID[x]][y]] + "\"");
                            setComputeTextColumn.calc = "CombineText";
                            setComputeTextColumn.refColumnName = refColumnName;
                            setComputeTextColumn.resultColumnName = conditionalJoinList.columnName[resultConditionalJoinColumnID[x]];

                            if (y > 1)
                            {
                                setComputeTextColumn.key2Value = tempKey2Value[x];
                                setComputeTextColumn.value2Key = tempValue2Key[x];
                            }

                            tableAfterConditionalJoin[y] = newTextColumn.calc(tableAfterConditionalJoin[y], setComputeTextColumn);
                            tempKey2Value[x] = tableAfterConditionalJoin[y].key2Value[tableAfterConditionalJoin[y].columnName.Count - 1];
                            tempValue2Key[x] = tableAfterConditionalJoin[y].value2Key[tableAfterConditionalJoin[y].columnName.Count - 1];
                        }
                        else
                        {
                            refColumnName.Clear();
                            refColumnName.Add(conditionalJoinList.factTable[resultConditionalJoinColumnID[x]][y].ToString());
                            setComputeColumn.calc = "Add";
                            setComputeColumn.refColumnName = refColumnName;
                            setComputeColumn.resultColumnName = conditionalJoinList.columnName[resultConditionalJoinColumnID[x]];
                            tableAfterConditionalJoin[y] = newColumn.calc(tableAfterConditionalJoin[y], setComputeColumn);
                        }
                    }
                }
            }

            ConcurrentQueue<int> checkJoinThreadCompleted = new ConcurrentQueue<int>();

            if (joinCurrentSetting.joinTableType.ToUpper() == "CONDITIONALJOIN")
            {
                ConcurrentDictionary<int, conditionalJoin> filterThread = new ConcurrentDictionary<int, conditionalJoin>();

                for (int worker = 1; worker < conditionalJoinRowCount; worker++)
                    filterThread.TryAdd(worker, new conditionalJoin());

                var options = new ParallelOptions()
                {
                    MaxDegreeOfParallelism = currentSetting.rowThread
                };
               
                Parallel.For(1, conditionalJoinRowCount, options, y =>
                {
                    tableAfterConditionalJoin[y] = conditionalJoinOneCondition(y, _matchedRow, checkJoinThreadCompleted, checkThreadCompleted, currentTable, conditionalJoinList, filterList, currentSetting, resultConditionalJoinColumnID, joinCurrentSetting, selectedColumnID, compareOperatorDict, reserveSpecialCharDict, newFilter, tempKey2Value, tempValue2Key);
                });
                
                do
                {
                    Thread.Sleep(5);                  
                } while (checkJoinThreadCompleted.Count < conditionalJoinRowCount - 1);
               
            }
           
            for (int s = 1; s < conditionalJoinRowCount; s++)
                for (int x = 0; x < tableAfterConditionalJoin[1].columnName.Count; x++)
                    tableAfterConditionalJoin[s].factTable[x].RemoveAt(0);            

            Dictionary<int, List<double>> resultfactTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < tableAfterConditionalJoin[conditionalJoinRowCount - 1].columnName.Count; x++)
            {
                resultfactTable.Add(x, new List<double>());
                resultfactTable[x].Add(x);

                for (int y = 1; y < conditionalJoinRowCount; y++)
                    resultfactTable[x].AddRange(tableAfterConditionalJoin[y].factTable[x]);
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = tableAfterConditionalJoin[conditionalJoinRowCount - 1].columnName;
            currentOutput.upperColumnName2ID = tableAfterConditionalJoin[conditionalJoinRowCount - 1].upperColumnName2ID;
            currentOutput.dataType = tableAfterConditionalJoin[conditionalJoinRowCount - 1].dataType;
            currentOutput.factTable = resultfactTable;
            currentOutput.key2Value = tableAfterConditionalJoin[conditionalJoinRowCount - 1].key2Value;
            currentOutput.value2Key = tableAfterConditionalJoin[conditionalJoinRowCount - 1].value2Key;

            return currentOutput;
        }

        public LedgerRAM conditionalJoinOneCondition(int y, Dictionary<int, int> _matchedRow, ConcurrentQueue<int> checkJoinThreadCompleted, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, LedgerRAM conditionalJoinList, LedgerRAM filterList, filterSetting currentSetting, List<int> resultConditionalJoinColumnID, conditionalJoinSetting joinCurrentSetting, List<int> selectedColumnID, Dictionary<string, string> compareOperatorDict, Dictionary<string, string> reserveSpecialCharDict, filter newFilter, Dictionary<int, Dictionary<double, string>> tempKey2Value, Dictionary<int, Dictionary<string, double>> tempValue2Key)
        {
            LedgerRAM tableAfterConditionalJoin = newFilter.filterByOneRowConditionList(y, _matchedRow, checkThreadCompleted, currentTable, filterList, currentSetting, selectedColumnID, compareOperatorDict, reserveSpecialCharDict);            
            Dictionary<int, List<double>> resultfactTable = new Dictionary<int, List<double>>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            var columnCount = tableAfterConditionalJoin.factTable.Count();

            for (int x = 0; x < columnCount; x++)
            {
                resultDataType.Add(x, tableAfterConditionalJoin.dataType[x]);
                resultColumnName.Add(x, tableAfterConditionalJoin.columnName[x]);
                resultUpperColumnName2ID.Add(tableAfterConditionalJoin.columnName[x].ToUpper(), x);
                resultfactTable.Add(x, tableAfterConditionalJoin.factTable[x]);

                if (tableAfterConditionalJoin.dataType[x] != "Number")
                {
                    resultKey2Value.Add(x, tableAfterConditionalJoin.key2Value[x]);
                    resultValue2Key.Add(x, tableAfterConditionalJoin.value2Key[x]);
                }
            }            

            for (int x = columnCount; x < (columnCount + resultConditionalJoinColumnID.Count); x++)
            {
                var currentColumnID = resultConditionalJoinColumnID[x - columnCount];

                resultDataType.Add(x, conditionalJoinList.dataType[currentColumnID]);
                resultColumnName.Add(x, conditionalJoinList.columnName[currentColumnID]);              
                resultUpperColumnName2ID.Add(conditionalJoinList.columnName[currentColumnID].ToUpper(), x);
                resultfactTable.Add(x, new List<double>());
                resultfactTable[x].Add(x);                

                if (conditionalJoinList.dataType[currentColumnID] != "Number")
                {
                    resultKey2Value.Add(x, conditionalJoinList.key2Value[currentColumnID]);
                    resultValue2Key.Add(x, conditionalJoinList.value2Key[currentColumnID]);

                    var currentText = conditionalJoinList.factTable[currentColumnID][y];

                    for (int i = 1; i < tableAfterConditionalJoin.factTable[0].Count; i++)
                    {
                       // Console.WriteLine(currentText);
                        resultfactTable[x].Add(currentText);
                    }
                }
                else
                {
                    var currentNumber = conditionalJoinList.factTable[currentColumnID][y];

                    for (int i = 1; i < tableAfterConditionalJoin.factTable[0].Count; i++)
                        resultfactTable[x].Add(currentNumber);                    
                }
            }

            tableAfterConditionalJoin.dataType = resultDataType;
            tableAfterConditionalJoin.columnName = resultColumnName;
            tableAfterConditionalJoin.factTable = resultfactTable;
            tableAfterConditionalJoin.upperColumnName2ID = resultUpperColumnName2ID;
            tableAfterConditionalJoin.key2Value = resultKey2Value;
            tableAfterConditionalJoin.value2Key = resultValue2Key;
          
            checkJoinThreadCompleted.Enqueue(y);

            return tableAfterConditionalJoin;
        }
    }
}