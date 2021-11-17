using System.Collections.Generic;

namespace MaxAccount
{
    public class date2EffectiveDateSetting
    {
        public int rowThread = 100;
        public List<string> dateColumnName { get; set; }
    }

    public class date2EffectiveDate
    {
        public LedgerRAM date2EffectiveDateProcess(LedgerRAM currentTable, date2EffectiveDateSetting currentSetting)
        {
            Dictionary<string, string> orderByColumnName = new Dictionary<string, string>();
            orderByColumnName.Add(currentSetting.dateColumnName[0], "D");

            orderBy newOrderBy = new orderBy();
            orderBySetting setOrderBy = new orderBySetting();
            setOrderBy.orderByColumnName = orderByColumnName;

            currentTable = newOrderBy.orderByList(currentTable, setOrderBy);

            int dateColumnID = currentTable.upperColumnName2ID[currentSetting.dateColumnName[0].ToUpper()];

            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            foreach (var pair in currentTable.key2Value[dateColumnID])
            {
                key2Value.Add(pair.Key, ">=" + pair.Value);
                value2Key.Add(">=" + pair.Value, pair.Key);
            }

            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.dataType[x] != "Number")
                {
                    if (x == dateColumnID)
                    {
                        resultKey2Value.Add(x, key2Value);
                        resultValue2Key.Add(x, value2Key);
                    }
                    else
                    {
                        resultKey2Value.Add(x, currentTable.key2Value[x]);
                        resultValue2Key.Add(x, currentTable.value2Key[x]);
                    }
                }
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = currentTable.factTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }       
    }
}

