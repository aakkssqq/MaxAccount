using System.Collections.Generic;

namespace MaxAccount
{
    public class appendRowSetting
    {
        public int rowThread = 100;
        public Dictionary<string, string> appendRow { get; set; }
    }

    public class appendRow
    {
        public LedgerRAM appendRowProcess(LedgerRAM currentTable, Dictionary<string, string> cellTextStore, Dictionary<string, double> cellNumberStore, appendRowSetting currentSetting)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            Dictionary<string, string> cellValue = new Dictionary<string, string>();
            Dictionary<int, string> columnID2Value = new Dictionary<int, string>();

            foreach (var pair in cellTextStore)            
                if(!cellValue.ContainsKey(pair.Key.ToUpper()))
                    cellValue.Add(pair.Key.ToUpper(), pair.Value);           

            foreach (var pair in cellNumberStore)           
                if (!cellValue.ContainsKey(pair.Key.ToUpper()))
                    cellValue.Add(pair.Key.ToUpper(), pair.Value.ToString());            

            foreach (var pair in currentSetting.appendRow)
            {
                if (cellValue.ContainsKey(pair.Value.ToUpper()))
                {
                    if(!columnID2Value.ContainsKey(upperColumnName2ID[pair.Key.ToUpper()]))
                        columnID2Value.Add(upperColumnName2ID[pair.Key.ToUpper()], cellValue[pair.Value.ToUpper()]);                   
                }
                else
                {
                    if (!columnID2Value.ContainsKey(upperColumnName2ID[pair.Key.ToUpper()]))
                        columnID2Value.Add(upperColumnName2ID[pair.Key.ToUpper()], pair.Value);                   
                }
            }

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                resultDataType.Add(x, currentTable.dataType[x]);
                resultColumnName.Add(x, currentTable.columnName[x].Trim());
                resultUpperColumnName2ID.Add(currentTable.columnName[x].Trim().ToUpper(), x);
                resultFactTable.Add(x, currentTable.factTable[x]);

                if (currentTable.dataType[x] != "Number")
                {
                    resultKey2Value.Add(x, currentTable.key2Value[x]);
                    resultValue2Key.Add(x, currentTable.value2Key[x]);
                }
            }

            int count;

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                var text = columnID2Value[x];

                if (currentTable.dataType[x] == "Number")
                {
                    bool success = double.TryParse(text, out double number);

                    if(success == true)
                        resultFactTable[x].Add(number);
                    else
                        resultFactTable[x].Add(0);
                }
                else
                {
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
