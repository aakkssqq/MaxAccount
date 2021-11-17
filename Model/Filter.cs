using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class filterSetting
    {
        public int rowThread = 100;
        public string filterType { get; set; }
        public List<string> selectedColumnName { get; set; }
        public Dictionary<int, List<string>> compareOperator { get; set; }
        public Dictionary<int, List<string>> selectedText { get; set; }             
        public Dictionary<int, List<double>> selectedNumber { get; set; }
        public Dictionary<int, List<string>> selectedTextNumber { get; set; }
        public string command { get; set; }
    }
    public class filter
    {
        public LedgerRAM filterByConditionList(LedgerRAM currentTable, LedgerRAM conditionList, filterSetting currentSetting)
        {   
            /*
            LedgerRAM2CSVdataFlow currentProcess = new LedgerRAM2CSVdataFlow();
            LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
            StringBuilder LEDGERRAM2CSV = currentProcess.LedgerRAM2CSV(conditionList, setLEDGERRAM2CSV);            

            using (StreamWriter toDisk = new StreamWriter("ddd.csv"))
            {   
                toDisk.Write(LEDGERRAM2CSV);
                toDisk.Close();
            }
            */

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

            for (int x = 0; x < conditionList.columnName.Count; x++)
                selectedColumnID.Add(upperColumnName2ID[conditionList.columnName[x].ToUpper()]);

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, LedgerRAM> tableMultithread = new ConcurrentDictionary<int, LedgerRAM>();

            for (int y = 1; y < conditionList.factTable[0].Count; y++)
                tableMultithread.TryAdd(y, new LedgerRAM());

            ConcurrentDictionary<int, filter> filterThread = new ConcurrentDictionary<int, filter>();

            for (int worker = 0; worker < conditionList.factTable[0].Count; worker++)
                filterThread.TryAdd(worker, new filter());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(1, conditionList.factTable[0].Count, options, y =>
            {
                tableMultithread[y] = filterByOneRowConditionList(y, null, checkThreadCompleted, currentTable, conditionList, currentSetting, selectedColumnID, compareOperatorDict, reserveSpecialCharDict);
            });

            do
            {
                Thread.Sleep(5);

            } while (checkThreadCompleted.Count < conditionList.factTable[0].Count - 1);            

            for (int s = 1; s <= tableMultithread.Count; s++)
                for (int x = 0; x < tableMultithread[1].columnName.Count; x++)
                    tableMultithread[s].factTable[x].RemoveAt(0);

            Dictionary<int, List<double>> resultfactTable = new Dictionary<int, List<double>>();          

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                resultfactTable.Add(x, new List<double>());
                  resultfactTable[x].Add(x);

                for (int y = 1; y < conditionList.factTable[0].Count; y++)               
                    resultfactTable[x].AddRange(tableMultithread[y].factTable[x]);
            }            

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultfactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
        public LedgerRAM filterByOneRowConditionList(int y, Dictionary<int, int> _matchedRow, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, LedgerRAM conditionList, filterSetting currentSetting, List<int> selectedColumnID, Dictionary<string, string> compareOperatorDict, Dictionary<string, string> reserveSpecialCharDict)
        {
            StringBuilder currentCell = new StringBuilder();
            List<string> selectedColumnName = new List<string>();
            Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();
            Dictionary<int, bool> isRangeExist = new Dictionary<int, bool>();
            int startAddress;
            bool isOperatorExist;

            for (int x = 0; x < selectedColumnID.Count; x++)
            {
                isOperatorExist = false;

                selectedColumnName.Add(conditionList.columnName[x].Trim());

                currentCell.Append(conditionList.key2Value[x][conditionList.factTable[x][y]]);

                startAddress = 0;

                if (!compareOperator.ContainsKey(x))
                    compareOperator.Add(x, new List<string>());

                if (!selectedTextNumber.ContainsKey(x))
                    selectedTextNumber.Add(x, new List<string>());

                for (int i = 0; i < currentCell.Length; i++)
                {
                    if (currentCell.Length >= 4 && i < currentCell.Length - 1)
                    {
                        if (compareOperatorDict.ContainsKey(currentCell.ToString().Substring(i, 2)))
                        {
                            if (currentCell.ToString().Substring(i, 2) == "..")
                            {
                                compareOperator[x].Add(">=");
                                compareOperator[x].Add("<=");
                                isOperatorExist = true;

                                if (!isRangeExist.ContainsKey(x))
                                    isRangeExist.Add(x, true);

                                if (!selectedTextNumber.ContainsKey(x))
                                    selectedTextNumber.Add(x, new List<string>());

                                selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, i - startAddress).Trim());
                            }
                            else
                            {
                                compareOperator[x].Add(currentCell.ToString().Substring(startAddress, 2).Trim());
                                isOperatorExist = true;
                            }

                            i++;
                            startAddress = i;
                        }
                    }

                    if (compareOperatorDict.ContainsKey(currentCell.ToString().Substring(i, 1)))
                    {
                        compareOperator[x].Add(currentCell.ToString().Substring(i, 1).Trim());
                        isOperatorExist = true;

                        if (!isRangeExist.ContainsKey(x))
                            isRangeExist.Add(x, false);

                        startAddress = i;
                    }

                    if (isOperatorExist == false && i == currentCell.Length - 1)
                        compareOperator[x].Add("=");

                    if (currentCell.ToString().Substring(i, 1) == ",")
                    {
                        if (isOperatorExist == false)
                            compareOperator[x].Add("=");

                        if (reserveSpecialCharDict.ContainsKey(currentCell.ToString().Substring(startAddress, 1)))
                        {
                            selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                        }
                        else
                        {
                            selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, i - startAddress).Trim());
                        }

                        startAddress = i;
                    }
                    else if (i == currentCell.Length - 1)
                    {
                        if (reserveSpecialCharDict.ContainsKey(currentCell.ToString().Substring(startAddress, 1)))
                        {
                            selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress + 1, currentCell.Length - startAddress - 1).Trim());
                        }
                        else
                        {
                            selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, currentCell.Length - startAddress).Trim());
                        }

                        startAddress = i;
                    }
                }
                currentCell.Clear();
            }

            filterSetting setFilter = new filterSetting();
            setFilter.filterType = currentSetting.filterType;
            setFilter.selectedColumnName = selectedColumnName;
            setFilter.compareOperator = compareOperator;
            setFilter.selectedTextNumber = selectedTextNumber;
           
            checkThreadCompleted.Enqueue(y);

            LedgerRAM currentOutput = new LedgerRAM();
          
            currentOutput = filterByOneRow(currentTable, setFilter, _matchedRow);         

            return currentOutput;
        }
        public LedgerRAM filterByOneRow(LedgerRAM currentTable, filterSetting currentSetting, Dictionary<int, int> _matchedRow)
        {                
            Dictionary<int, int> selectedColumnIDdict = new Dictionary<int, int>();
            List<string> selectedUpperColumnName = new List<string>();

            for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
                selectedUpperColumnName.Add(currentSetting.selectedColumnName[x].ToUpper());

            for (int i = 0; i < selectedUpperColumnName.Count; i++)
                selectedColumnIDdict.Add(currentTable.upperColumnName2ID[selectedUpperColumnName[i]], i);

            List<string> selectedColumnName = new List<string>();
            Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<string>> selectedText = new Dictionary<int, List<string>>();
            Dictionary<int, List<double>> selectedNumber = new Dictionary<int, List<double>>();
            Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();

            var list = selectedColumnIDdict.Keys.ToList();
            list.Sort();

            int u = 0;
            foreach (var key in list)
            {
                selectedColumnName.Add(currentSetting.selectedColumnName[selectedColumnIDdict[key]]);
                compareOperator.Add(u, currentSetting.compareOperator[selectedColumnIDdict[key]]);
                selectedTextNumber.Add(u, currentSetting.selectedTextNumber[selectedColumnIDdict[key]]);
                u++;
            }

            for (int i = 0; i < selectedColumnName.Count; i++)
            {
                if (currentTable.dataType[currentTable.upperColumnName2ID[selectedColumnName[i].ToUpper()]] == "Number")
                {
                    selectedNumber.Add(i, new List<double>());

                    for (int j = 0; j < selectedTextNumber[i].Count; j++)
                    {
                        var isNumber = double.TryParse(selectedTextNumber[i][j], out double number);

                        if (isNumber == true)
                            selectedNumber[i].Add(number);
                    }
                }
                else
                {
                    selectedText.Add(i, new List<string>());

                    for (int j = 0; j < selectedTextNumber[i].Count; j++)
                    {
                        selectedText[i].Add(selectedTextNumber[i][j]);
                    }
                }
            }

            currentSetting.selectedColumnName = selectedColumnName;
            currentSetting.compareOperator = compareOperator;
            currentSetting.selectedText = selectedText;
            currentSetting.selectedNumber = selectedNumber;           

            List<int> selectedColumnID = new List<int>();         
          
            for (int i = 0; i < currentSetting.selectedColumnName.Count; i++)
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.selectedColumnName[i].ToUpper()))
                    selectedColumnID.Add(currentTable.upperColumnName2ID[currentSetting.selectedColumnName[i].ToUpper()]);          

            condition2Exact currentCondition2Exact = new condition2Exact();
            condition2ExactSetting setCondition2Exact = new condition2ExactSetting();
            setCondition2Exact.filterType = currentSetting.filterType;
            setCondition2Exact.selectedColumnName = selectedColumnName;
            setCondition2Exact.compareOperator = compareOperator;
            setCondition2Exact.selectedText = selectedText;          
          
            Dictionary<int, Dictionary<double, string>> matchedKey = currentCondition2Exact.condition2ExactProcess(currentTable, setCondition2Exact);           

            LedgerRAM currentOutput = filterByMultiThread(false, currentTable, currentSetting, selectedColumnID, matchedKey, _matchedRow);         

            return currentOutput;
        }
        public LedgerRAM filterByDistinctList(LedgerRAM currentTable, LedgerRAM distinctList, filterSetting currentSetting)
        {
            List<string> originalColumnName = new List<string>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
                originalColumnName.Add(currentTable.columnName[x]);

            List<string> originalDistinctColumnName = new List<string>();
            for (int x = 0; x < distinctList.columnName.Count; x++)
                originalDistinctColumnName.Add(distinctList.columnName[x]);


            List<string> distinctNumberColumnName = new List<string>();
            List<string> distinctTextColumnName = new List<string>();

            for (int x = 0; x < distinctList.columnName.Count; x++)
            {
                if (distinctList.dataType[x] == "Number")
                {
                    distinctNumberColumnName.Add(distinctList.columnName[x]);
                    distinctTextColumnName.Add("Text:" + distinctList.columnName[x]);
                }
                else
                {
                    distinctTextColumnName.Add(distinctList.columnName[x]);
                }
            }

            if (distinctNumberColumnName.Count > 0)
            {
                number2Text newNumber2Text = new number2Text();
                number2TextSetting setNumber2Text = new number2TextSetting();
                setNumber2Text.number2Text = distinctNumberColumnName;

                distinctList = newNumber2Text.number2TextList(distinctList, setNumber2Text);

                selectColumn newSelectColumn = new selectColumn();
                selectColumnSetting setSelectColumn = new selectColumnSetting();
                setSelectColumn.selectColumn = distinctTextColumnName;
                distinctList = newSelectColumn.selectColumnName(distinctList, setSelectColumn);                

                number2Text newNumber2Text2 = new number2Text();
                number2TextSetting setNumber2Text2 = new number2TextSetting();
                setNumber2Text2.number2Text = originalDistinctColumnName;

                currentTable = newNumber2Text.number2TextList(currentTable, setNumber2Text);                
            }

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<int> selectedColumnID = new List<int>();

            for (int x = 0; x < distinctList.columnName.Count; x++)
                selectedColumnID.Add(upperColumnName2ID[distinctList.columnName[x].ToUpper()]);                        
            
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            Dictionary<int, LedgerRAM> tableMultithread = new Dictionary<int, LedgerRAM>();

            for (int y = 1; y < distinctList.factTable[0].Count; y++)
                tableMultithread.Add(y, new LedgerRAM());

            ConcurrentDictionary<int, filter> filterThread = new ConcurrentDictionary<int, filter>();

            for (int worker = 0; worker < distinctList.factTable[0].Count; worker++)
                filterThread.TryAdd(worker, new filter());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(1, distinctList.factTable[0].Count, options, y =>
            {
                tableMultithread[y] = filterByDistinctListSegment(y, checkThreadCompleted, currentTable, distinctList, currentSetting, selectedColumnID);               
            });           

            do
            {
                Thread.Sleep(5);            

            } while (checkThreadCompleted.Count < distinctList.factTable[0].Count - 1);

            Dictionary<int, List<double>> resultfactTable = new Dictionary<int, List<double>>();
            
            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                resultfactTable.Add(x, new List<double>());
                resultfactTable[x].Add(x);

                for (int y = 1; y < distinctList.factTable[0].Count; y++)
                    resultfactTable[x].AddRange(tableMultithread[y].factTable[x]);              
            }          

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultfactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;


            if (distinctNumberColumnName.Count > 0)
            {
                selectColumn newSelectColumn2 = new selectColumn();
                selectColumnSetting setSelectColumn2 = new selectColumnSetting();
                setSelectColumn2.selectColumn = originalColumnName;
                currentOutput = newSelectColumn2.selectColumnName(currentOutput, setSelectColumn2);
            }
           

            return currentOutput;
        }
        public LedgerRAM filterByDistinctListSegment(int y, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, LedgerRAM distinctList, filterSetting currentSetting, List<int> selectedColumnID)
        {
            condition2Exact currentCondition2Exact = new condition2Exact();
            condition2ExactSetting setCondition2Exact = new condition2ExactSetting();
            List<string> selectedColumnName = new List<string>();
            Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<string>> selectedText = new Dictionary<int, List<string>>();

            for (int x = 0; x < distinctList.columnName.Count; x++)
            {
                selectedColumnName.Add(distinctList.columnName[x]);
                compareOperator.Add(x, new List<string>());
                compareOperator[x].Add("=");
                selectedText.Add(x, new List<string>());
                selectedText[x].Add(distinctList.key2Value[x][distinctList.factTable[x][y]]);                
            }

            setCondition2Exact.filterType = currentSetting.filterType;
            setCondition2Exact.selectedColumnName = selectedColumnName;
            setCondition2Exact.compareOperator = compareOperator;
            setCondition2Exact.selectedText = selectedText;

            Dictionary<int, Dictionary<double, string>> matchedKey = currentCondition2Exact.condition2ExactProcess(currentTable, setCondition2Exact);

            LedgerRAM currentOutput = filterByMultiThread(true, currentTable, currentSetting, selectedColumnID, matchedKey, null);            
            checkThreadCompleted.Enqueue(y);

            return currentOutput;
        }
        public LedgerRAM filterByMultiThread(bool isFilterByDistinctList, LedgerRAM currentTable, filterSetting currentSetting, List<int> selectedColumnID, Dictionary<int, Dictionary<double, string>> matchedKey, Dictionary<int, int> _matchedRow)
        {
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, filter> concurrentRowSegment = new ConcurrentDictionary<int, filter>();
            ConcurrentDictionary<int, Dictionary<int, List<double>>> filteredfactTableSegment = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            Dictionary<int, List<double>> filteredfactTable = new Dictionary<int, List<double>>();
            ConcurrentDictionary<int, Dictionary<int, int>> matchedRowSegment = new ConcurrentDictionary<int, Dictionary<int, int>>();


            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                filteredfactTable.Add(x, new List<double>());

                if(isFilterByDistinctList == false)
                    filteredfactTable[x].Add(x);
            }

            List<int> rowSegment = new List<int>();
           
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

            for (int worker = 0; worker < rowSegment.Count - 1; worker++)
            {
                concurrentRowSegment.TryAdd(worker, new filter());
                filteredfactTableSegment.TryAdd(worker, new Dictionary<int, List<double>>());
                matchedRowSegment.TryAdd(worker, new Dictionary<int, int>());
            }

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                (filteredfactTableSegment[currentSegment], matchedRowSegment[currentSegment]) = filterValueSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, selectedColumnID, matchedKey, _matchedRow);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                for (int i = 0; i < rowSegment.Count - 1; i++)
                    filteredfactTable[x].AddRange(filteredfactTableSegment[i][x]);
            }

            Dictionary<int, int> matchedRow = new Dictionary<int, int>();

            for (int s = 0; s < matchedRowSegment.Count; s++)
                foreach (var pair in matchedRowSegment[s])               
                    matchedRow.Add(pair.Key, pair.Value);           

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = filteredfactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;
            currentOutput.matchedRow = matchedRow;

            return currentOutput;
        }
        public (Dictionary<int, List<double>>, Dictionary<int, int>) filterValueSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, filterSetting currentSetting, List<int> selectedColumnID, Dictionary<int, Dictionary<double, string>> matchedKey, Dictionary<int, int> _matchedRow)
        {
            Dictionary<int, int> matchedRow = new Dictionary<int, int>();
            Dictionary<int, List<double>> filteredfactTable = new Dictionary<int, List<double>>();            

            for (int x = 0; x < currentTable.columnName.Count; x++)           
                filteredfactTable.Add(x, new List<double>());

            bool isStatisifedCondition;
            int orNumber;

            if (currentSetting.filterType == "And")
            {
                if (_matchedRow == null)
                {
                    for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                    {                        
                        isStatisifedCondition = true;

                        for (int x = 0; x < selectedColumnID.Count; x++)
                            if (isStatisifedCondition == true && currentTable.dataType[selectedColumnID[x]] != "Number")
                                isStatisifedCondition = isStatisifedCondition & matchedKey[x].ContainsKey(currentTable.factTable[selectedColumnID[x]][y]);

                        for (int x = 0; x < selectedColumnID.Count; x++)
                        {
                            if (isStatisifedCondition == true && currentTable.dataType[selectedColumnID[x]] == "Number")
                            {
                                for (int z = 0; z < currentSetting.selectedNumber[x].Count; z++)
                                {
                                    if (currentSetting.compareOperator[x][z] == ">=")
                                        if (currentSetting.selectedNumber[x][z] > currentTable.factTable[selectedColumnID[x]][y])
                                            isStatisifedCondition = false;

                                    if (currentSetting.compareOperator[x][z] == ">")
                                        if (currentSetting.selectedNumber[x][z] >= currentTable.factTable[selectedColumnID[x]][y])
                                            isStatisifedCondition = false;

                                    if (currentSetting.compareOperator[x][z] == "<=")
                                        if (currentSetting.selectedNumber[x][z] < currentTable.factTable[selectedColumnID[x]][y])
                                            isStatisifedCondition = false;

                                    if (currentSetting.compareOperator[x][z] == "<")
                                        if (currentSetting.selectedNumber[x][z] <= currentTable.factTable[selectedColumnID[x]][y])
                                            isStatisifedCondition = false;

                                    if (currentSetting.compareOperator[x][z] == "!=")
                                    {                                       
                                        if (currentSetting.selectedNumber[x][z] == currentTable.factTable[selectedColumnID[x]][y])
                                        {
                                          //  Console.WriteLine("false");
                                            isStatisifedCondition = false;
                                        }
                                    }
                                }

                                orNumber = 0;

                                for (int z = 0; z < currentSetting.selectedNumber[x].Count; z++)
                                {
                                    if (currentSetting.compareOperator[x][z] == "=")
                                    {
                                        if (currentTable.factTable[selectedColumnID[x]][y] == currentSetting.selectedNumber[x][z])
                                            orNumber++;

                                        if (z == currentSetting.selectedNumber[x].Count - 1 && orNumber == 0)
                                            isStatisifedCondition = false;
                                    }
                                }
                            }
                        }
                        if (isStatisifedCondition == true)
                        {
                            for (int x = 0; x < currentTable.columnName.Count; x++)
                            {
                                filteredfactTable[x].Add(currentTable.factTable[x][y]);

                                if (x == 0)
                                    matchedRow.Add(y, y);
                            }
                        }
                        
                    }
                }
                if (_matchedRow != null)
                {
                    for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                    {                   
                        if (!_matchedRow.ContainsKey(y))
                        {
                            isStatisifedCondition = true;

                            for (int x = 0; x < selectedColumnID.Count; x++)
                                if (isStatisifedCondition == true && currentTable.dataType[selectedColumnID[x]] != "Number")
                                    isStatisifedCondition = isStatisifedCondition & matchedKey[x].ContainsKey(currentTable.factTable[selectedColumnID[x]][y]);

                            for (int x = 0; x < selectedColumnID.Count; x++)
                            {
                                if (isStatisifedCondition == true && currentTable.dataType[selectedColumnID[x]] == "Number")
                                {
                                    for (int z = 0; z < currentSetting.selectedNumber[x].Count; z++)
                                    {
                                        if (currentSetting.compareOperator[x][z] == ">=")
                                            if (currentSetting.selectedNumber[x][z] > currentTable.factTable[selectedColumnID[x]][y])
                                                isStatisifedCondition = false;

                                        if (currentSetting.compareOperator[x][z] == ">")
                                            if (currentSetting.selectedNumber[x][z] >= currentTable.factTable[selectedColumnID[x]][y])
                                                isStatisifedCondition = false;

                                        if (currentSetting.compareOperator[x][z] == "<=")
                                            if (currentSetting.selectedNumber[x][z] < currentTable.factTable[selectedColumnID[x]][y])
                                                isStatisifedCondition = false;

                                        if (currentSetting.compareOperator[x][z] == "<")
                                            if (currentSetting.selectedNumber[x][z] <= currentTable.factTable[selectedColumnID[x]][y])
                                                isStatisifedCondition = false;

                                        if (currentSetting.compareOperator[x][z] == "!=")
                                      
                                            if (currentSetting.selectedNumber[x][z] != currentTable.factTable[selectedColumnID[x]][y])                                           
                                                isStatisifedCondition = false;                                           
                                      
                                    }

                                    orNumber = 0;

                                    for (int z = 0; z < currentSetting.selectedNumber[x].Count; z++)
                                    {
                                        if (currentSetting.compareOperator[x][z] == "=")
                                        {
                                            if (currentTable.factTable[selectedColumnID[x]][y] == currentSetting.selectedNumber[x][z])
                                                orNumber++;

                                            if (z == currentSetting.selectedNumber[x].Count - 1 && orNumber == 0)
                                                isStatisifedCondition = false;
                                        }
                                    }
                                }
                            }
                            if (isStatisifedCondition == true)
                            {
                                for (int x = 0; x < currentTable.columnName.Count; x++)
                                {
                                    filteredfactTable[x].Add(currentTable.factTable[x][y]);

                                    if (x == 0)
                                        matchedRow.Add(y, y);
                                }
                            }
                        }
                    }
                }
            }

            if (currentSetting.filterType == "Or")
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    isStatisifedCondition = false;
                    
                    for (int x = 0; x < selectedColumnID.Count; x++)
                        if (isStatisifedCondition == false && currentTable.dataType[selectedColumnID[x]] != "Number")                       
                            if (matchedKey[x].ContainsKey(currentTable.factTable[selectedColumnID[x]][y]) == true)
                                isStatisifedCondition = true;                      

                    for (int x = 0; x < selectedColumnID.Count; x++)
                        if (isStatisifedCondition == false && currentTable.dataType[selectedColumnID[x]] == "Number")
                        {
                            for (int z = 0; z < currentSetting.selectedNumber[x].Count; z++)
                            {
                                if (currentSetting.compareOperator[x][z] == ">=")
                                    if (currentSetting.selectedNumber[x][z] < currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;

                                if (currentSetting.compareOperator[x][z] == ">")
                                    if (currentSetting.selectedNumber[x][z] <= currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;

                                if (currentSetting.compareOperator[x][z] == "<=")
                                    if (currentSetting.selectedNumber[x][z] > currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;

                                if (currentSetting.compareOperator[x][z] == "<")
                                    if (currentSetting.selectedNumber[x][z] >= currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;

                                if (currentSetting.compareOperator[x][z] == "!=")
                                    if (currentSetting.selectedNumber[x][z] != currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;

                                if (currentSetting.compareOperator[x][z] == "=")
                                    if (currentSetting.selectedNumber[x][z] == currentTable.factTable[selectedColumnID[x]][y])
                                        isStatisifedCondition = true;
                            }
                        }

                    if (isStatisifedCondition == true)
                        for (int x = 0; x < currentTable.columnName.Count; x++)
                        {
                            filteredfactTable[x].Add(currentTable.factTable[x][y]);

                            if (x == 0)
                                matchedRow.Add(y, y);
                        }
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return (filteredfactTable, matchedRow);
        }

    }
}
