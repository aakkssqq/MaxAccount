using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class amendColumnNameSetting
    {        
        public string sourceColumnName { get; set; }
        public string resultColumnName { get; set; }
    }

    public class amendColumnName
    {
        public LedgerRAM amendColumnNameProcess(LedgerRAM currentTable, amendColumnNameSetting currentSetting)
        {
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.columnName[x].ToUpper() == currentSetting.sourceColumnName.ToUpper())
                {
                    columnName.Add(x, currentSetting.resultColumnName);
                    upperColumnName2ID.Add(currentSetting.resultColumnName.ToUpper(), x);
                }
                else
                {
                    columnName.Add(x, currentTable.columnName[x]);
                    upperColumnName2ID.Add(currentTable.columnName[x].ToUpper(), x);
                }
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = columnName;
            currentOutput.upperColumnName2ID = upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = currentTable.factTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
      
    }
}