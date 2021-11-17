using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class voucherEntrySetting
    {     
        public List<string> account { get; set; }
        public List<string> drCr { get; set; }
        public List<string> amount { get; set; }
    }

    public class voucherEntry
    {
        public LedgerRAM voucherEntryProcess(LedgerRAM currentTable, voucherEntrySetting currentSetting)
        {
            string balanceAccount = null;

            Dictionary<string, string> dcDict = new Dictionary<string, string>();
            dcDict.Add("D", "D");
            dcDict.Add("DR", "D");
            dcDict.Add("DEBIT", "D");
            dcDict.Add("C", "C");
            dcDict.Add("CR", "C");
            dcDict.Add("CREDIT", "C");
            dcDict.Add("B", "B");
            dcDict.Add("BAL", "B");
            dcDict.Add("BALANCE", "B");
            dcDict.Add("BALANCE ENTRY", "B");            

            List<string> drCr = new List<string>();
            List<string> excludeBalanceGroupBy = new List<string>();            

            List<int> removeKey = new List<int>();

            for (int x = 0; x < currentSetting.drCr.Count; x++)
            {
                if (currentSetting.drCr[x].ToUpper() == "EXCLUDEBALANCEGROUPBY")
                {
                    excludeBalanceGroupBy.Add(currentSetting.account[x].ToUpper());
                    removeKey.Add(x);                  
                }
            }   

            for (int x = 0; x < currentSetting.drCr.Count - removeKey.Count; x++)
            {
                if (dcDict.ContainsKey(currentSetting.drCr[x].ToUpper()))
                {
                    drCr.Add(dcDict[currentSetting.drCr[x].ToUpper()]);

                    if (dcDict[currentSetting.drCr[x].ToUpper()] == "B")
                        balanceAccount = currentSetting.account[x];
                }
            }

            List<string> unmatchColumn = new List<string>();
            List<int> unmatchColumnDrCr = new List<int>();
            List<string> refColumnName = new List<string>();
            string drCrAmountColumn = null;
            List<string> tableName = new List<string>();
            LedgerRAM amendTable = new LedgerRAM();

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            Dictionary<string, LedgerRAM> ramStore = new Dictionary<string, LedgerRAM>();
            copyTable newcopyTable = new copyTable();

            computeTextColumn newTextColumn = new computeTextColumn();
            computeTextColumnSetting setComputeTextColumn = new computeTextColumnSetting();

            computeColumn newNumberColumn = new computeColumn();
            computeColumnSetting setComputeColumn = new computeColumnSetting();

            for (int x = 0; x < drCr.Count; x++)
            {
                if (upperColumnName2ID.ContainsKey(currentSetting.account[x].ToUpper()))
                {
                    refColumnName.Clear();
                    refColumnName.Add("\"" + currentSetting.account[x] + "\"");
                    drCrAmountColumn = currentSetting.account[x];
                    setComputeTextColumn.calc = "CombineText";
                    setComputeTextColumn.refColumnName = refColumnName;
                    setComputeTextColumn.resultColumnName = "Account";
                    amendTable = newTextColumn.calc(currentTable, setComputeTextColumn);

                    refColumnName.Clear();
                    refColumnName.Add("\"" + drCr[x] + "\"");                  
                    setComputeTextColumn.calc = "CombineText";
                    setComputeTextColumn.refColumnName = refColumnName;
                    setComputeTextColumn.resultColumnName = "D/C";
                    amendTable = newTextColumn.calc(amendTable, setComputeTextColumn);

                    refColumnName.Clear();
                    refColumnName.Add(currentSetting.account[x]);                    
                    setComputeColumn.calc = "Add";
                    setComputeColumn.refColumnName = refColumnName;
                    setComputeColumn.resultColumnName = currentSetting.amount[0];
                    amendTable = newNumberColumn.calc(amendTable, setComputeColumn);

                    tableName.Add(currentSetting.account[x]);                    
                    ramStore.Add(currentSetting.account[x], newcopyTable.copyTableProcess(amendTable));                    
                }
                else
                {
                    unmatchColumn.Add(currentSetting.account[x]);
                    unmatchColumnDrCr.Add(x);
                }
            }

            for (int x = 0; x < unmatchColumn.Count; x++)
            {
                if (drCr.Count == 2 && !drCr.Contains("B") && unmatchColumn.Count == 1)
                {
                    refColumnName.Clear();                   
                    refColumnName.Add("\"" + unmatchColumn[x] + "\"");                    
                    setComputeTextColumn.calc = "CombineText";
                    setComputeTextColumn.refColumnName = refColumnName;
                    setComputeTextColumn.resultColumnName = "Account";
                    amendTable = newTextColumn.calc(currentTable, setComputeTextColumn);

                    refColumnName.Clear();
                    refColumnName.Add("\"" + drCr[unmatchColumnDrCr[x]] + "\"");                    
                    setComputeTextColumn.calc = "CombineText";
                    setComputeTextColumn.refColumnName = refColumnName;
                    setComputeTextColumn.resultColumnName = "D/C";
                    amendTable = newTextColumn.calc(amendTable, setComputeTextColumn);

                    refColumnName.Clear();
                    refColumnName.Add(drCrAmountColumn);                    
                    setComputeColumn.calc = "Add";
                    setComputeColumn.refColumnName = refColumnName;
                    setComputeColumn.resultColumnName = currentSetting.amount[0];
                    amendTable = newNumberColumn.calc(amendTable, setComputeColumn);

                    tableName.Add(unmatchColumn[x]);
                    ramStore.Add(unmatchColumn[x], newcopyTable.copyTableProcess(amendTable));
                }
            }            

            Dictionary<string, LedgerRAM> ramStoreAfterRemoveColumn = new Dictionary<string, LedgerRAM>();

            List<string> selectColumn = new List<string>();

            for (int x = 0; x < amendTable.columnName.Count; x++)
            {
                if (amendTable.columnName[x] != drCrAmountColumn)                    
                    selectColumn.Add(amendTable.columnName[x]);
            }

            selectColumn newSelectColumn = new selectColumn();
            selectColumnSetting setSelectColumn = new selectColumnSetting();
            setSelectColumn.selectColumn = selectColumn;

            foreach (var table in ramStore)
                ramStoreAfterRemoveColumn.Add(table.Key, newSelectColumn.selectColumnName(table.Value, setSelectColumn));           

            mergeTable newMergeTable = new mergeTable();
            mergeTableSetting setMergeTable = new mergeTableSetting();
            setMergeTable.tableName = tableName;          

            LedgerRAM combineDrCrTable = new LedgerRAM();
            Dictionary<string, LedgerRAM> combineVoucher = new Dictionary<string, LedgerRAM>();

            combineDrCrTable = newMergeTable.mergeTableProcess(ramStore, setMergeTable);           
            combineVoucher.Add("1", combineDrCrTable);

            LedgerRAM balanceTable = new LedgerRAM();
            
            if (drCr.Contains("B"))
            {
                selectColumn.Clear();
                for (int x = 0; x < amendTable.columnName.Count; x++)
                {
                    if (amendTable.columnName[x] != "Account")
                        selectColumn.Add(amendTable.columnName[x]);
                }
               
                setSelectColumn.selectColumn = selectColumn;
                combineDrCrTable = newSelectColumn.selectColumnName(combineDrCrTable, setSelectColumn);

                balanceTable = newcopyTable.copyTableProcess(combineDrCrTable);            

                DC newDC2NegativePositive = new DC();
                DCsetting setDC2NegativePositive = new DCsetting();
                setDC2NegativePositive.DC2NegativePositive = currentSetting.amount;
                balanceTable = newDC2NegativePositive.DC2Number(balanceTable, setDC2NegativePositive);

                List<string> groupByColumnName = new List<string>();
                List<string> aggregateByColumnName = new List<string>();
                List<string> aggregateFunction = new List<string>();

                for (int x = 0; x < balanceTable.columnName.Count; x++)
                {
                    if (balanceTable.dataType[x] == "Number")
                    {
                        aggregateFunction.Add("Sum");
                        aggregateByColumnName.Add(balanceTable.columnName[x]);                       
                    }
                    else if(!excludeBalanceGroupBy.Contains(balanceTable.columnName[x].ToUpper()))
                    {
                        groupByColumnName.Add(balanceTable.columnName[x]);
                    }
                } 

                groupBy newGroupBy = new groupBy();
                groupBySetting setGroupBy = new groupBySetting();
                setGroupBy.groupByColumnName = groupByColumnName;
                setGroupBy.aggregateFunction = aggregateFunction;
                setGroupBy.aggregateByColumnName = aggregateByColumnName;
                balanceTable = newGroupBy.groupByList(balanceTable, setGroupBy);

                refColumnName.Clear();
                refColumnName.Add("\"" + balanceAccount + "\"");
                setComputeTextColumn.calc = "CombineText";
                setComputeTextColumn.refColumnName = refColumnName;
                setComputeTextColumn.resultColumnName = "Account";
                balanceTable = newTextColumn.calc(balanceTable, setComputeTextColumn);
              
                DC newPositiveNegative2DC = new DC();
                DCsetting setPositiveNegative2DC = new DCsetting();
                setPositiveNegative2DC.PositiveNegative2DC = currentSetting.amount;
                balanceTable = newPositiveNegative2DC.number2DC(balanceTable, setPositiveNegative2DC);

                combineVoucher.Add("2", balanceTable);

                tableName.Clear();
                tableName.Add("1");
                tableName.Add("2");
                setMergeTable.tableName = tableName;
                combineDrCrTable = newMergeTable.mergeTableProcess(combineVoucher, setMergeTable);
            } 

            return combineDrCrTable;
        }
    }
}
