using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class selectColumnSetting
    {
        public int rowThread = 100;
        public List<string> selectColumn { get; set; }
        public string selectType { get; set; }
    }

    public class selectColumn
    {
        public LedgerRAM selectColumnName(LedgerRAM currentTable, selectColumnSetting currentSetting)
        {             
            List<string> selectColumnName = new List<string>();
            List<int> selectColumnID = new List<int>();

            if (currentSetting.selectType != "Remove")
            {

                for (int x = 0; x < currentSetting.selectColumn.Count; x++)
                {
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.selectColumn[x].ToUpper()))
                    {
                        selectColumnName.Add(currentSetting.selectColumn[x]);
                        selectColumnID.Add(currentTable.upperColumnName2ID[currentSetting.selectColumn[x].ToUpper()]);
                    }
                }
            }

            if (currentSetting.selectType == "Remove")
            {
                List<string> upperRemoveColumnName = new List<string>();

                for (int x = 0; x < currentSetting.selectColumn.Count; x++)
                    upperRemoveColumnName.Add(currentSetting.selectColumn[x].ToUpper());

                for (int x = 0; x < currentTable.columnName.Count; x++)
                {
                    if (!upperRemoveColumnName.Contains(currentTable.columnName[x].ToUpper()))
                    {   
                        selectColumnName.Add(currentTable.columnName[x]);
                        selectColumnID.Add(currentTable.upperColumnName2ID[currentTable.columnName[x].ToUpper()]);
                    }
                }
            }

            currentTable = reorderColumn(selectColumnID, currentTable);            

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = currentTable.factTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
        public LedgerRAM reorderColumn(List<int> selectColumnID, LedgerRAM currentTable)
        {           

            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, int> new2OldSelectColumnID = new Dictionary<int, int>();

            for (int x = 0; x < selectColumnID.Count; x++)
                new2OldSelectColumnID.Add(x, selectColumnID[x]);

            selectColumnID.Clear();

            for (int x = 0; x < new2OldSelectColumnID.Count; x++)
            {
                selectColumnID.Add(x);
                var currentID = Convert.ToInt32(new2OldSelectColumnID[x]);
                columnName.Add(x, currentTable.columnName[currentID]);
                upperColumnName2ID.Add(currentTable.columnName[currentID].ToUpper(), x);
                dataType.Add(x, currentTable.dataType[currentID]);
                factTable.Add(x, currentTable.factTable[currentID]);
                factTable[x][0] = x;

                if (currentTable.dataType[currentID] != "Number")
                {
                    key2Value.Add(x, currentTable.key2Value[currentID]);
                    value2Key.Add(x, currentTable.value2Key[currentID]);
                }
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = columnName;
            currentOutput.upperColumnName2ID = upperColumnName2ID;
            currentOutput.dataType = dataType;
            currentOutput.factTable = factTable;
            currentOutput.key2Value = key2Value;
            currentOutput.value2Key = value2Key;

            return currentOutput;
        }       
    }
}
