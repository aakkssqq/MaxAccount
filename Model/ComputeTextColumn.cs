using System.Collections.Generic;

namespace MaxAccount
{
    public class computeTextColumnSetting
    {        
        public string calc { get; set; }
        public List<string> refColumnName { get; set; }
        public List<int> refColumnID { get; set; }
        public List<double> refConstant { get; set; }
        public bool isFirstContant { get; set; }
        public string resultColumnName { get; set; }

        public int decimalPlace = 999;       
        public Dictionary<double, string> key2Value { get; set; } 
        public Dictionary<string, double> value2Key { get; set; } 
    }

    public class computeTextColumn
    {
        public LedgerRAM calc(LedgerRAM currentTable, computeTextColumnSetting currentSetting)
        {           
            Dictionary<int, string> refColumnID2Text = new Dictionary<int, string>();           

            currentSetting.isFirstContant = false;
            List<int> refColumnID = new List<int>();

            for (int i = 0; i < currentSetting.refColumnName.Count; i++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.refColumnName[i].ToUpper()))
                {
                    refColumnID.Add(currentTable.upperColumnName2ID[currentSetting.refColumnName[i].ToUpper()]);
                    refColumnID2Text.Add(i, "null");
                }

                if (currentSetting.refColumnName[i].Substring(0, 1) == "\"" && currentSetting.refColumnName[i].Substring(currentSetting.refColumnName[i].Length - 1, 1) == "\"")
                {
                    refColumnID.Add(-(i + 1));
                    refColumnID2Text.Add(-(i + 1), currentSetting.refColumnName[i].Substring(1, currentSetting.refColumnName[i].Length - 2));
                }
            }        
            
            currentSetting.refColumnID = refColumnID;           

            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            if (currentSetting.key2Value != null)
            {
                foreach (var pair in currentSetting.key2Value)
                    key2Value.Add(pair.Key, pair.Value);

                foreach (var pair in currentSetting.value2Key)
                    value2Key.Add(pair.Key, pair.Value);
            }                       

            int resultColumnID = currentTable.columnName.Count;
            List<double> factTable = new List<double>();
            factTable.Add(resultColumnID);
          
            string currentText;
            int count;

            for (int y = 1; y < currentTable.factTable[0].Count; y++)
            {
                currentText = null;

                for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                {
                    if (currentSetting.refColumnID[x] >= 0)
                        if (currentTable.dataType[currentSetting.refColumnID[x]] != "Number")
                            currentText = currentText + currentTable.key2Value[currentSetting.refColumnID[x]][currentTable.factTable[currentSetting.refColumnID[x]][y]];
                        else
                            currentText = currentText + currentTable.factTable[currentSetting.refColumnID[x]][y].ToString();

                    if (currentSetting.refColumnID[x] < 0)
                        currentText = currentText + refColumnID2Text[currentSetting.refColumnID[x]];
                }

                if (value2Key.ContainsKey(currentText)) // 
                    factTable.Add(value2Key[currentText]);

                else
                {
                    count = value2Key.Count;
                    key2Value.Add(count, currentText);
                    value2Key.Add(currentText, count);
                    factTable.Add(count);
                }
            }

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                resultDataType.Add(i, currentTable.dataType[i]);
                resultColumnName.Add(i, currentTable.columnName[i].Trim());
                resultUpperColumnName2ID.Add(currentTable.columnName[i].Trim().ToUpper(), i);
                resultFactTable.Add(i, currentTable.factTable[i]);

                if (currentTable.dataType[i] != "Number")
                {
                    resultKey2Value.Add(i, currentTable.key2Value[i]);
                    resultValue2Key.Add(i, currentTable.value2Key[i]);
                }
            }
           
            resultDataType.Add(resultColumnID, "Text");
            resultColumnName.Add(resultColumnID, currentSetting.resultColumnName);
            resultUpperColumnName2ID.Add(currentSetting.resultColumnName.ToUpper(), resultColumnID);           
            resultFactTable.Add(resultColumnID, factTable);
            resultKey2Value.Add(resultColumnID, key2Value);
            resultValue2Key.Add(resultColumnID, value2Key);

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.dataType = resultDataType;
            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;           
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }       
    
    }
}
