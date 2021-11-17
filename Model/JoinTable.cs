using System.Collections.Generic;
using System.Threading.Tasks;
using MaxAccountExtension;

namespace MaxAccount
{
    public class joinTableSetting
    {
        public int columnThread = 100;
        public string leftTable { get; set; }
        public string rightTable { get; set; }
        public List<string> leftTableColumn { get; set; }
        public List<string> rightTableColumn { get; set; }  
        public string joinTableType { get; set; }
    }
    
    public class joinTable
    {
        public LedgerRAM joinTableList(LedgerRAM leftTable, LedgerRAM rightTable, joinTableSetting currentSetting)
        {
            currentSetting.rightTable = null;
            currentSetting.leftTable = null;

            Dictionary<int, string> revisedColumnName = new Dictionary<int, string>();

            for (int x = 0; x < rightTable.columnName.Count; x++)
                if (currentSetting.rightTableColumn.Contains(rightTable.columnName[x]))               
                    revisedColumnName.Add(x, currentSetting.leftTableColumn[x]);
                else                
                    revisedColumnName.Add(x, rightTable.columnName[x]);

            rightTable.columnName = revisedColumnName;

            List<string> sameColumn = new List<string>();

            for (int x = 0; x < currentSetting.leftTableColumn.Count; x++)
                sameColumn.Add(currentSetting.leftTableColumn[x]);

            currentSetting.rightTableColumn = sameColumn;            

            int unmatchKeyAddress = rightTable.factTable[0].Count;

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            List<int> commonTableColumnID = new List<int>();
            List<string> commonTableUpperColumnName = new List<string>();

            foreach (var pair in leftTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            for (int x = 0; x < currentSetting.leftTableColumn.Count; x++)
            {
                if (upperColumnName2ID.ContainsKey(currentSetting.leftTableColumn[x].ToUpper()))
                {
                    commonTableColumnID.Add(upperColumnName2ID[currentSetting.rightTableColumn[x].ToUpper()]);
                    commonTableUpperColumnName.Add(currentSetting.leftTableColumn[x].ToUpper());
                }
            }        

            List<int> revisedRightTableColumnID = new List<int>();
            for (int x = 0; x < commonTableColumnID.Count; x++)
                revisedRightTableColumnID.Add(x);

            unifyTable currentUnifyTable = new unifyTable();
            unifyTableSetting setUnifyTable = new unifyTableSetting();
            setUnifyTable.leftTableColumn = currentSetting.leftTableColumn;
            setUnifyTable.rightTableColumn = currentSetting.rightTableColumn;
            setUnifyTable.commonTableColumnID = commonTableColumnID;
            setUnifyTable.commonTableUpperColumnName = commonTableUpperColumnName;
            setUnifyTable.revisedRightTableColumnID = revisedRightTableColumnID;
           
            rightTable = currentUnifyTable.unifyTableProcess(leftTable, rightTable, setUnifyTable);           

            Dictionary<string, int> leftUpperColumnName2ID = new Dictionary<string, int>();
         
            foreach (var pair in leftTable.columnName)
                leftUpperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<int> leftTableColumnID = new List<int>();

            for (int x = 0; x < currentSetting.leftTableColumn.Count; x++)
                if (leftTable.upperColumnName2ID.ContainsKey(currentSetting.leftTableColumn[x].ToUpper()))
                    leftTableColumnID.Add(leftUpperColumnName2ID[currentSetting.leftTableColumn[x].ToUpper()]);
         
            compositeKey currentCompositeKey = new compositeKey();            
            compositeKeySetting currentCompositeKeySetting = new compositeKeySetting();

            // LeftTable to compositeKeyList
            currentCompositeKeySetting.compositeColumnID = leftTableColumnID;
            currentCompositeKeySetting.factTable = leftTable.factTable;
            currentCompositeKeySetting.key2Value = leftTable.key2Value;
            Task<List<decimal>> leftTableCompositeKeyTask = new Task<List<decimal>>(() =>
            {
                return currentCompositeKey.compositeKeyList(currentCompositeKeySetting);
            });
            leftTableCompositeKeyTask.Start();
            List<decimal> leftTableCompositeKeyList = leftTableCompositeKeyTask.Result;

            // RightTable to compositeKeyDict
            currentCompositeKeySetting.compositeColumnID = revisedRightTableColumnID;           
            currentCompositeKeySetting.factTable = rightTable.factTable;
            currentCompositeKeySetting.key2Value = rightTable.key2Value;
            Task<Dictionary<decimal, int>> rightTableCompositeKeyTask = new Task<Dictionary<decimal, int>>(() =>
            {
                return currentCompositeKey.compositeKeyDict(currentCompositeKeySetting);
            });
            rightTableCompositeKeyTask.Start();
            Dictionary<decimal, int> rightTableCompositeKeyDict = rightTableCompositeKeyTask.Result;
           
            List<int> unuseRightTableRowFullJoin = new List<int>();
            if (currentSetting.joinTableType == "FullJoin")
            {               
                // LeftTable to compositeKeyDict
                currentCompositeKeySetting.compositeColumnID = leftTableColumnID;
                currentCompositeKeySetting.factTable = leftTable.factTable;
                currentCompositeKeySetting.key2Value = leftTable.key2Value;
                Task<Dictionary<decimal, int>> leftTableCompositeKeyTaskFullJoin = new Task<Dictionary<decimal, int>>(() =>
                {
                    return currentCompositeKey.compositeKeyDict(currentCompositeKeySetting);
                });
                leftTableCompositeKeyTaskFullJoin.Start();
                Dictionary<decimal, int> leftTableCompositeKeyDictFullJoin = leftTableCompositeKeyTaskFullJoin.Result;

                // RightTable to compositeKeyDict
                currentCompositeKeySetting.compositeColumnID = revisedRightTableColumnID;
                currentCompositeKeySetting.factTable = rightTable.factTable;
                currentCompositeKeySetting.key2Value = rightTable.key2Value;               
                Task <List<decimal>> rightTableCompositeKeyTaskFullJoin = new Task<List<decimal>>(() =>
                {
                    return currentCompositeKey.compositeKeyList(currentCompositeKeySetting);
                });
                rightTableCompositeKeyTaskFullJoin.Start();
                List<decimal> rightTableCompositeKeyListFullJoin = rightTableCompositeKeyTaskFullJoin.Result;                

                Dictionary<int, int> unmatchRightTableDictFullJoin = new Dictionary<int, int>();               

                for (int y = 0; y < rightTableCompositeKeyListFullJoin.Count; y++) //lookup value               
                    if (!leftTableCompositeKeyDictFullJoin.ContainsKey(rightTableCompositeKeyListFullJoin[y]))
                        unuseRightTableRowFullJoin.Add(y);

                leftTableCompositeKeyTaskFullJoin.Wait(); rightTableCompositeKeyTaskFullJoin.Wait();
            }           

            leftTableCompositeKeyTask.Wait(); rightTableCompositeKeyTask.Wait();

            Dictionary<int, int> unmatchLeftTableDict = new Dictionary<int, int>();
            List<int> joinKeyList = new List<int>();

            for (int y = 0; y < leftTableCompositeKeyList.Count; y++) //lookup value
            {
                if (rightTableCompositeKeyDict.ContainsKey(leftTableCompositeKeyList[y]))
                {
                    joinKeyList.Add(rightTableCompositeKeyDict[leftTableCompositeKeyList[y]]); // nth row of right table                  
                }
                else
                {
                    if (currentSetting.joinTableType == "JoinTable" || currentSetting.joinTableType == "FullJoin")
                        joinKeyList.Add(-1);

                    if (currentSetting.joinTableType == "InnerJoin")
                        unmatchLeftTableDict.Add(y+1, y+1);                    
                }
            }

            Dictionary<string, LedgerRAM> ramStore = new Dictionary<string, LedgerRAM>();
            
            splitTable currentSplit = new splitTable();
            splitTableSetting setSplitTable = new splitTableSetting();           

            if (currentSetting.joinTableType == "InnerJoin")
            {
                setSplitTable.unmatchTableDict = unmatchLeftTableDict;

                if (unmatchLeftTableDict.Count > 0)
                    ramStore = currentSplit.splitTableByKey(leftTable, rightTable, setSplitTable);               
            }           

            //Combine Left and Right Table
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();            

            for (int x = 0; x < leftTable.columnName.Count; x++)
            {
                resultDataType.Add(x, leftTable.dataType[x]);
                resultColumnName.Add(x, leftTable.columnName[x].Trim());
                resultUpperColumnName2ID.Add(leftTable.columnName[x].Trim().ToUpper(), x);

                if (currentSetting.joinTableType == "InnerJoin" && unmatchLeftTableDict.Count > 0)
                    resultFactTable.Add(x, ramStore["match"].factTable[x]);
                else
                    resultFactTable.Add(x, leftTable.factTable[x]);

                if (leftTable.dataType[x] != "Number")
                {
                    resultKey2Value.Add(x, leftTable.key2Value[x]);
                    resultValue2Key.Add(x, leftTable.value2Key[x]);
                }
            }

            List<int> joinRightTableColumnID = new List<int>();

            for (int x = 0; x < rightTable.columnName.Count; x++)
            {
                if (!revisedRightTableColumnID.Contains(x))
                    joinRightTableColumnID.Add(x);
            }            
          
            for (int x = leftTable.columnName.Count; x < leftTable.columnName.Count + joinRightTableColumnID.Count; x++)
            {
                int _joinRightTableColumnID = joinRightTableColumnID[x - leftTable.columnName.Count];

                resultDataType.Add(x, rightTable.dataType[_joinRightTableColumnID]);
                resultColumnName.Add(x, rightTable.columnName[_joinRightTableColumnID]);
                resultUpperColumnName2ID.Add(rightTable.columnName[_joinRightTableColumnID].ToUpper(), x);

                if (currentSetting.joinTableType == "InnerJoin")
                    resultFactTable[x] = joinTargetColumnInnerJoin(x, rightTable, _joinRightTableColumnID, joinKeyList, unmatchKeyAddress);

                if (currentSetting.joinTableType == "JoinTable" || currentSetting.joinTableType == "FullJoin")
                    resultFactTable[x] = joinTargetColumnLeftJoin(x, rightTable, _joinRightTableColumnID, joinKeyList, unmatchKeyAddress);

                if (rightTable.dataType[_joinRightTableColumnID] != "Number")
                {
                    resultKey2Value.Add(x, rightTable.key2Value[_joinRightTableColumnID]);
                    resultValue2Key.Add(x, rightTable.value2Key[_joinRightTableColumnID]);
                }
            }

            if (currentSetting.joinTableType == "FullJoin") // Add Row for Full Join
            {                
                List<int> commonColumnID = new List<int>();
                List<int> commonRightColumnID = new List<int>();
                Dictionary<string, int> rightUpperColumnName2ID = new Dictionary<string, int>();

                for (int x = 0; x < resultColumnName.Count; x++)
                {
                    if (commonTableUpperColumnName.Contains(resultColumnName[x].ToUpper()))
                        commonColumnID.Add(x);
                }

                for (int x = 0; x < rightTable.columnName.Count; x++)
                {
                    rightUpperColumnName2ID.Add(rightTable.columnName[x].ToUpper(), x);

                    if (commonTableUpperColumnName.Contains(rightTable.columnName[x].ToUpper()))
                        commonRightColumnID.Add(x);
                }

                string text = null;
                int unmatchKey = 0;

                for (int y = 0; y < unuseRightTableRowFullJoin.Count; y++)
                {
                    for (int x = 0; x < resultColumnName.Count; x++)
                    {
                        if (!rightUpperColumnName2ID.ContainsKey(resultColumnName[x].ToUpper()))
                        {
                            if (resultDataType[x] == "Number")                            
                                resultFactTable[x].Add(0);
                            else
                            {
                                if (resultDataType[x] == "Text")
                                    text = "Unmatch";

                                if (resultDataType[x] == "Date")
                                    text = "0";

                                if (resultDataType[x] == "Period")
                                    text = "1900p01";

                                if (resultValue2Key[x].ContainsKey(text)) // same master record                               
                                    resultFactTable[x].Add(resultValue2Key[x][text]);
                                
                                else // add new master record
                                {
                                    unmatchKey = resultValue2Key[x].Count;
                                    resultKey2Value[x].Add(unmatchKey, text);
                                    resultValue2Key[x].Add(text, unmatchKey);
                                    resultFactTable[x].Add(unmatchKey);                                  
                                }
                            }
                        }
                        else
                        {
                            var currentRightColumnID = rightUpperColumnName2ID[resultColumnName[x].ToUpper()];
                            var unuseRightTableCell = rightTable.factTable[currentRightColumnID][unuseRightTableRowFullJoin[y] + 1];                           

                            if (rightTable.dataType[currentRightColumnID] != "Number")
                            {
                                if (commonColumnID.Contains(x))
                                {                                  
                                    text = resultKey2Value[x][unuseRightTableCell];

                                    if (resultValue2Key[x].ContainsKey(text)) // same master record                                  
                                        resultFactTable[x].Add(resultValue2Key[x][text]);
                                }
                                else
                                {
                                     text = rightTable.key2Value[currentRightColumnID][unuseRightTableCell];
                                     resultFactTable[x].Add(unuseRightTableCell);                                  
                                }
                            }   
                            else
                            {                              
                                resultFactTable[x].Add(unuseRightTableCell);                               
                            }
                        }
                    }
                }          
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = new Dictionary<int, string>(resultColumnName);
            currentOutput.upperColumnName2ID = new Dictionary<string, int>(resultUpperColumnName2ID);
            currentOutput.dataType = new Dictionary<int, string>(resultDataType);
            currentOutput.factTable = new Dictionary<int, List<double>>(resultFactTable);            
            currentOutput.key2Value = new Dictionary<int, Dictionary<double, string>>(resultKey2Value);
            currentOutput.value2Key = new Dictionary<int, Dictionary<string, double>>(resultValue2Key);

            return currentOutput;
        }
        public List<double> joinTargetColumnInnerJoin(int x, LedgerRAM rightTable, int _joinRightTableColumnID, List<int> joinKeyList, int unmatchKeyAddress)
        {
            List<double> resultFactTable = new List<double>();           
            resultFactTable.Add(x);

            for (int y = 0; y < joinKeyList.Count; y++)              
                 resultFactTable.Add(rightTable.factTable[_joinRightTableColumnID][joinKeyList[y]]);               

            return resultFactTable;
        }
        public List<double> joinTargetColumnLeftJoin(int x, LedgerRAM rightTable, int _joinRightTableColumnID, List<int> joinKeyList, int unmatchKeyAddress)
        {
            List<double> resultFactTable = new List<double>();
            resultFactTable.Add(x);

            for (int y = 0; y < joinKeyList.Count; y++)
                if (joinKeyList[y] != -1)
                    resultFactTable.Add(rightTable.factTable[_joinRightTableColumnID][joinKeyList[y]]);
                else
                    resultFactTable.Add(rightTable.factTable[_joinRightTableColumnID][unmatchKeyAddress]);

            return resultFactTable;
        }
    }
}