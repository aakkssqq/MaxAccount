using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class mergeTableSetting
    {
        public int columnThread = 100;

        public int rowThread = 100;
        public List<string> tableName { get; set; }
        public string sourceTable { get; set; }
        public string resultTable { get; set; }
    }

    public class mergeTable
    {
        public LedgerRAM mergeCommonTableProcess(Dictionary<string, LedgerRAM> ramStore, mergeTableSetting currentSetting)
        {         
            Dictionary<int, List<double>> sourceFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();            

            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {
                var currentTable = currentSetting.tableName[i];              

                for (int x = 0; x < ramStore[currentTable].columnName.Count; x++)
                {
                    if (!sourceFactTable.ContainsKey(x))
                    {
                        sourceFactTable.Add(x, new List<double>());
                        sourceFactTable[x].AddRange(ramStore[currentTable].factTable[x]);
                    }

                    if (!resultFactTable.ContainsKey(x))
                        resultFactTable.Add(x, new List<double>());

                    if (i > 0)                    
                        sourceFactTable[x].RemoveAt(0);

                    resultFactTable[x].AddRange(sourceFactTable[x]);

                    sourceFactTable.Clear();
                }                
            }            

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = ramStore[currentSetting.tableName[0]].columnName;
            currentOutput.upperColumnName2ID = ramStore[currentSetting.tableName[0]].upperColumnName2ID;
            currentOutput.dataType = ramStore[currentSetting.tableName[0]].dataType;
            currentOutput.factTable = new Dictionary<int, List<double>>(resultFactTable);
            currentOutput.key2Value = ramStore[currentSetting.tableName[0]].key2Value;
            currentOutput.value2Key = ramStore[currentSetting.tableName[0]].value2Key;

            return currentOutput;
        }
        public LedgerRAM mergeTableProcess(Dictionary<string, LedgerRAM> ramStore, mergeTableSetting currentSetting)
        {
            Dictionary<string, Dictionary<string, int>> upperColumnName2ID = new Dictionary<string, Dictionary<string, int>>();
            Dictionary<string, List<int>> missingColumnID = new Dictionary<string, List<int>>();
            string currentTable;

            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {
                currentTable = currentSetting.tableName[i];               

                upperColumnName2ID.Add(currentTable, new Dictionary<string, int>());
                missingColumnID.Add(currentTable, new List<int>());

                foreach (var pair in ramStore[currentTable].columnName)                
                    upperColumnName2ID[currentTable].Add(pair.Value.ToUpper(), pair.Key);
            }

            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();

            int u = 0;
            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {
                currentTable = currentSetting.tableName[i];

                for (int x = 0; x < ramStore[currentTable].columnName.Count; x++)
                {
                    if (!resultUpperColumnName2ID.ContainsKey(ramStore[currentTable].columnName[x].ToUpper()))
                    {
                        resultUpperColumnName2ID.Add(ramStore[currentTable].columnName[x].ToUpper(), u);
                        resultColumnName.Add(u, ramStore[currentTable].columnName[x]);
                        resultDataType.Add(u, ramStore[currentTable].dataType[x]);                        
                        u++;
                    } 
                }
            }          

            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {
                currentTable = currentSetting.tableName[i];

                for (int x = 0; x < resultColumnName.Count; x++)                
                    if (!upperColumnName2ID[currentTable].ContainsKey(resultColumnName[x].ToUpper()))
                        missingColumnID[currentTable].Add(x);
            }

            List<string> refColumnName = new List<string>();            
            computeColumn newColumn = new computeColumn();
            computeColumnSetting setComputeColumn = new computeColumnSetting();
            computeTextColumn newTextColumn = new computeTextColumn();
            computeTextColumnSetting setComputeTextColumn = new computeTextColumnSetting();
            Dictionary<string, LedgerRAM> tableStore = new Dictionary<string, LedgerRAM>();
            
            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {                
                currentTable = currentSetting.tableName[i];

                if (!tableStore.ContainsKey(currentTable))
                {
                    tableStore.Add(currentTable, new LedgerRAM());
                    tableStore[currentTable] = ramStore[currentTable];
                }

                for (int x = 0; x < missingColumnID[currentTable].Count; x++)
                {
                    if (resultDataType[missingColumnID[currentTable][x]] == "Number")
                    {                       
                        setComputeColumn.calc = "Add";
                        refColumnName.Clear();
                        refColumnName.Add("0");
                        setComputeColumn.refColumnName = refColumnName;
                        setComputeColumn.resultColumnName = resultColumnName[missingColumnID[currentTable][x]];                        
                        tableStore[currentTable] = newColumn.calc(tableStore[currentTable], setComputeColumn);
                    }
                    else
                    {  
                        setComputeTextColumn.calc = "CombineText";
                        refColumnName.Clear();
                        refColumnName.Add("\"null\"");
                        setComputeTextColumn.refColumnName = refColumnName;
                        setComputeTextColumn.resultColumnName = resultColumnName[missingColumnID[currentTable][x]];                       
                        tableStore[currentTable] = newTextColumn.calc(tableStore[currentTable], setComputeTextColumn);
                    }
                }
            }

            for (int i = 0; i < currentSetting.tableName.Count; i++)
            {
                if(!tableStore.ContainsKey(currentSetting.tableName[i]))
                    tableStore.Add(currentSetting.tableName[i], ramStore[currentSetting.tableName[i]]);
            }
          
            List<string> selectColumn = new List<string>();

            for (int x = 0; x < resultColumnName.Count; x++)            
                selectColumn.Add(resultColumnName[x]);                

            selectColumn newSelectColumn = new selectColumn();
            selectColumnSetting setSelectColumn = new selectColumnSetting();
            setSelectColumn.selectColumn = selectColumn;

            for (int i = 0; i < currentSetting.tableName.Count; i++)
                tableStore[currentSetting.tableName[i]] = newSelectColumn.selectColumnName(tableStore[currentSetting.tableName[i]], setSelectColumn);           

            copyTable newcopyTable = new copyTable();
            copyTableSetting setCopyTable2 = new copyTableSetting();

            for (int i = 0; i < currentSetting.tableName.Count - 1; i++)
            {
                setCopyTable2.sourceTable = currentSetting.tableName[i + 1];
                setCopyTable2.resultTable = currentSetting.resultTable;
                setCopyTable2.commonTable = currentSetting.tableName[i];
                tableStore[currentSetting.tableName[i + 1]] = newcopyTable.copyTableCommonTableProcess(tableStore, setCopyTable2);
            }         
            
            Dictionary<string, LedgerRAM> tableStore2 = new Dictionary<string, LedgerRAM>();

            tableStore2.Add(currentSetting.tableName[0], tableStore[currentSetting.tableName[0]]);

            for (int i = 0; i < currentSetting.tableName.Count - 1; i++)           
                tableStore2.Add(currentSetting.tableName[i + 1], tableStore[currentSetting.tableName[i + 1]]);          

            mergeTableSetting setMergeTable = new mergeTableSetting();
            setMergeTable.tableName = currentSetting.tableName;           
           
            return mergeCommonTableProcess(tableStore2, setMergeTable);
        }
        public LedgerRAM combineTableByCommonColumnProcess(Dictionary<string, LedgerRAM> ramStore, mergeTableSetting currentSetting)
        {
            List<string> allColumn = new List<string>();           
            List<string> commonColumn = new List<string>();
            Dictionary<string, List<string>> upperColumnNameByTable = new Dictionary<string, List<string>>();

            foreach (var tableName in currentSetting.tableName)
            {
                upperColumnNameByTable.Add(tableName, new List<string>());

                for (int x = 0; x < ramStore[tableName].columnName.Count; x++)
                {
                    upperColumnNameByTable[tableName].Add(ramStore[tableName].columnName[x].ToUpper());

                    if (!allColumn.Contains(ramStore[tableName].columnName[x]))
                        allColumn.Add(ramStore[tableName].columnName[x]);
                }
            }
          
            bool isMatchColumnName;

            for (int x = 0; x < allColumn.Count; x++)
            {
                isMatchColumnName = true;

                foreach (var pair in upperColumnNameByTable)
                {
                    if (!pair.Value.Contains(allColumn[x].ToUpper()))
                        isMatchColumnName = false;
                }

                if (isMatchColumnName == true)
                    commonColumn.Add(allColumn[x]);
            }

            Dictionary<string, LedgerRAM> commonColumnRamStore = new Dictionary<string, LedgerRAM>();

            foreach (var tableName in currentSetting.tableName)
            {
                selectColumn newSelectColumn = new selectColumn();
                selectColumnSetting setSelectColumn = new selectColumnSetting();
                setSelectColumn.selectColumn = commonColumn;
                commonColumnRamStore.Add(tableName, newSelectColumn.selectColumnName(ramStore[tableName], setSelectColumn));                
            }
                      

            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            Dictionary<string, LedgerRAM> backupRamStore = new Dictionary<string, LedgerRAM>();

            copyTable newcopyTable = new copyTable();           
           
            foreach (var pair in currentSetting.tableName)
               backupRamStore.Add(pair, newcopyTable.copyTableProcess(commonColumnRamStore[pair]));

            string text = null;
            int count;           

            for (int x = 0; x < commonColumnRamStore[currentSetting.tableName[0]].columnName.Count; x++)
            {
                resultColumnName.Add(x, commonColumnRamStore[currentSetting.tableName[0]].columnName[x]);
                resultUpperColumnName2ID.Add(commonColumnRamStore[currentSetting.tableName[0]].columnName[x].ToUpper(), x);
                resultDataType.Add(x, commonColumnRamStore[currentSetting.tableName[0]].dataType[x]);
                resultFactTable.Add(x, new List<double>());
                resultFactTable[x].Add(x);

                if (commonColumnRamStore[currentSetting.tableName[0]].dataType[x] == "Number")
                {
                    foreach (var pair in currentSetting.tableName)
                    {
                        backupRamStore[pair].factTable[x].RemoveAt(0);
                        resultFactTable[x].AddRange(backupRamStore[pair].factTable[x]);                       
                    }
                }
                else
                {
                    resultKey2Value.Add(x, new Dictionary<double, string>());
                    resultValue2Key.Add(x, new Dictionary<string, double>());

                    foreach (var pair in currentSetting.tableName)
                    {
                        for (int y = 1; y < commonColumnRamStore[pair].factTable[x].Count; y++)
                        {
                            text = commonColumnRamStore[pair].key2Value[x][commonColumnRamStore[pair].factTable[x][y]].ToString();

                            if (resultValue2Key[x].ContainsKey(text))
                                resultFactTable[x].Add(resultValue2Key[x][text]);

                            else
                            {
                                count = resultValue2Key[x].Count;
                                resultKey2Value[x].Add(count, text);
                                resultValue2Key[x].Add(text, count);
                                resultFactTable[x].Add(count);
                            }
                        }
                    }
                }
            }

            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }
    }
}

