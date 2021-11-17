using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MaxAccountExtension;

namespace MaxAccount
{
    public class crosstabSetting
    {
        public int rowThread = 100;
        public List<string> xColumnName { get; set; }
        public List<string> yColumnName { get; set; }
        public List<string> crosstabAggregateFunction { get; set; }
        public List<string> crosstabAggregateByColumnName { get; set; }
        public string command { get; set; }
        public string currentSQLServer { get; set; }
        public string currentConnectionString { get; set; }
        public string sourceTable { get; set; }
        public string resultTable { get; set; }
        public List<string> selectedColumnName { get; set; }
    }

    public class crosstab
    {
        public LedgerRAM crosstabTable(LedgerRAM currentTable, crosstabSetting currentSetting)
        {           
            List<string> crosstabAggregateByUpperColumnName = new List<string>();
            List<string> xUpperColume = new List<string>();
            List<string> yUpperColume = new List<string>();

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)
                xUpperColume.Add(currentSetting.xColumnName[x].ToUpper());

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                yUpperColume.Add(currentSetting.yColumnName[x].ToUpper());

            for (int x = 0; x < currentSetting.crosstabAggregateByColumnName.Count; x++)
                crosstabAggregateByUpperColumnName.Add(currentSetting.crosstabAggregateByColumnName[x].ToUpper());

            LedgerRAM groupByList = new LedgerRAM();
           
            groupByList = groupBy(currentTable, currentSetting);           

            List<string> xColumeName = new List<string>();
            List<string> yColumeName = new List<string>();

            Dictionary<string, int> groupBycolumnNameDict = new Dictionary<string, int>();

            foreach (var pair in groupByList.columnName)           
                groupBycolumnNameDict.Add(pair.Value.ToUpper(), pair.Key);

            xUpperColume.Clear();
            yUpperColume.Clear();

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)
            {
                if (groupBycolumnNameDict.ContainsKey("TEXT:" + currentSetting.xColumnName[x].ToUpper()))
                {
                    xColumeName.Add("Text:" + currentSetting.xColumnName[x]);
                    xUpperColume.Add("TEXT:" + currentSetting.xColumnName[x].ToUpper());
                }
                else
                {
                    xColumeName.Add(currentSetting.xColumnName[x]);
                    xUpperColume.Add(currentSetting.xColumnName[x].ToUpper());
                }
            }

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
            {
                if (groupBycolumnNameDict.ContainsKey("TEXT:" + currentSetting.yColumnName[x].ToUpper()))
                {
                    yColumeName.Add("Text:" + currentSetting.yColumnName[x]);
                    yUpperColume.Add("TEXT:" + currentSetting.yColumnName[x].ToUpper());
                }
                else
                {
                    yColumeName.Add(currentSetting.yColumnName[x]);
                    yUpperColume.Add(currentSetting.yColumnName[x].ToUpper());
                }
            }

            currentSetting.xColumnName = xColumeName;
            currentSetting.yColumnName = yColumeName;

            Task<LedgerRAM> xDistinctListTask = new Task<LedgerRAM>(() =>
            {
                return xDistinct(groupByList, currentSetting);                 
            });
            xDistinctListTask.Start();
            LedgerRAM xDistinctList = xDistinctListTask.Result;

            Task<LedgerRAM> yDistinctListTask = new Task<LedgerRAM>(() =>
            {
                return yDistinct(groupByList, currentSetting); 
            });
            yDistinctListTask.Start();
            LedgerRAM yDistinctList = yDistinctListTask.Result;

            xDistinctListTask.Wait(); yDistinctListTask.Wait();

            List<int> xColumnID = new List<int>();
            List<int> yColumnID = new List<int>();
            Dictionary<int, List<double>> xColumnFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> yColumnFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> xColumnKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<double, string>> yColumnKey2Value = new Dictionary<int, Dictionary<double, string>>();

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)          
                xColumnID.Add(x);

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)           
                yColumnID.Add(x);

            int u = 0; int vx = 0; int vy = 0;          

            foreach (var pair in groupByList.upperColumnName2ID)
            {
                if (xUpperColume.Contains(pair.Key))
                {
                    xColumnFactTable.Add(vx, groupByList.factTable[u]);                    
                    xColumnKey2Value.Add(vx, groupByList.key2Value[u]);
                    vx++;
                }
                if (yUpperColume.Contains(pair.Key))
                {                    
                    yColumnFactTable.Add(vy, groupByList.factTable[u]);
                    yColumnKey2Value.Add(vy, groupByList.key2Value[u]);
                    vy++;
                }
                u++;
            }

            compositeKey currentCompositeKey = new compositeKey();
            compositeKeySetting currentCompositeKeySetting = new compositeKeySetting();

            currentCompositeKeySetting.compositeColumnID = xColumnID;
            currentCompositeKeySetting.factTable = xDistinctList.factTable;
            currentCompositeKeySetting.key2Value = xDistinctList.key2Value;

            // create x, y address
            Task<Dictionary<decimal, int>> xDistinctCheckSumDictTask = new Task<Dictionary<decimal, int>>(() =>
            {
                return currentCompositeKey.compositeKeyDict(currentCompositeKeySetting);             
            });
            xDistinctCheckSumDictTask.Start();
            Dictionary<decimal, int> xDistinctCheckSumDict = xDistinctCheckSumDictTask.Result;

            currentCompositeKeySetting.compositeColumnID = yColumnID;
            currentCompositeKeySetting.factTable = yDistinctList.factTable;
            currentCompositeKeySetting.key2Value = yDistinctList.key2Value;

            Task<Dictionary<decimal, int>> yDistinctCheckSumDictTask = new Task<Dictionary<decimal, int>>(() =>
            {
                return currentCompositeKey.compositeKeyDict(currentCompositeKeySetting);             
            });
            yDistinctCheckSumDictTask.Start();
            Dictionary<decimal, int> yDistinctCheckSumDict = yDistinctCheckSumDictTask.Result;

            currentCompositeKeySetting.compositeColumnID = xColumnID;
            currentCompositeKeySetting.factTable = xColumnFactTable;
            currentCompositeKeySetting.key2Value = xColumnKey2Value;

            Task<List<decimal>> xFactTableCheckSumListTask = new Task<List<decimal>>(() =>
            {
                return currentCompositeKey.compositeKeyList(currentCompositeKeySetting);              
            });
            xFactTableCheckSumListTask.Start();
            List<decimal> xFactTableCheckSumList = xFactTableCheckSumListTask.Result;

            currentCompositeKeySetting.compositeColumnID = yColumnID;
            currentCompositeKeySetting.factTable = yColumnFactTable;
            currentCompositeKeySetting.key2Value = yColumnKey2Value;

            Task<List<decimal>> yFactTableCheckSumListTask = new Task<List<decimal>>(() =>
            {
                return currentCompositeKey.compositeKeyList(currentCompositeKeySetting);               
            });
            yFactTableCheckSumListTask.Start();
            List<decimal> yFactTableCheckSumList = yFactTableCheckSumListTask.Result;
          
            xDistinctCheckSumDictTask.Wait(); yDistinctCheckSumDictTask.Wait(); xFactTableCheckSumListTask.Wait(); yFactTableCheckSumListTask.Wait();

            u = 0;
            int xAddressColID = groupByList.factTable.Count;
            groupByList.factTable.Add(xAddressColID, new List<double>());
            groupByList.factTable[xAddressColID].Add(xAddressColID);
            foreach (var key in xFactTableCheckSumList)
            {
                u++;
                groupByList.factTable[xAddressColID].Add(xDistinctCheckSumDict[key]);
            }
            u = 0;
            int yAddressColID = groupByList.factTable.Count;
            groupByList.factTable.Add(yAddressColID, new List<double>());
            groupByList.factTable[yAddressColID].Add(yAddressColID);
            foreach (var key in yFactTableCheckSumList)
            {
                u++;
                groupByList.factTable[yAddressColID].Add(yDistinctCheckSumDict[key]);
            }
          
            groupByList.columnName.Add(xAddressColID, "xAddress");            
            groupByList.columnName.Add(yAddressColID, "yAddress");
            groupByList.upperColumnName2ID.Add("xAddress", xAddressColID);
            groupByList.upperColumnName2ID.Add("yAddress", yAddressColID);
            groupByList.dataType.Add(xAddressColID, "Number");
            groupByList.dataType.Add(yAddressColID, "Number");           

            // create crosstab numberical body
            Dictionary<int, List<double>> YXMcrosstabTable = new Dictionary<int, List<double>>();
            int numberColCount = currentSetting.crosstabAggregateByColumnName.Count;
            int start0Col = yDistinctList.factTable.Count;
            int end0Col = yDistinctList.factTable.Count + ((xDistinctList.factTable[0].Count - 1) * numberColCount);

            for (int i = 0; i < yDistinctList.factTable.Count; i++)            
                YXMcrosstabTable[i] = yDistinctList.factTable[i];
           
            List<double> source = new List<double>();
            for (int j = 0; j < yDistinctList.factTable[0].Count; j++)
                source.Add(0);

            for (int i = start0Col; i < end0Col; i++) // add zero for X and M column
            {
                YXMcrosstabTable.Add(i, new List<double>());
                YXMcrosstabTable[i].AddRange(source);               
            }
           
            int targetCol;
            int targetRow;
            int yColCount = yDistinctList.columnName.Count;
            u = 0;

            for (int xValue = 1; xValue < xDistinctList.factTable[0].Count; xValue++)     
            {
                for (int m = 0; m < currentSetting.crosstabAggregateByColumnName.Count; m++) // read distinctList number and write to YXMcrosstabTable Table Header
                {
                    targetCol = start0Col + ((xValue - 1) * numberColCount) + m;                
                    YXMcrosstabTable[targetCol][0] = yColCount + u;
                    u++;
                }
            }

            for (int m = numberColCount; m >= 1; m--) // read distinctList number and write to YXMcrosstabTable Table Body
            {
                for (int YXMrow = 1; YXMrow < yColumnFactTable[0].Count; YXMrow++)
                {
                    targetCol = start0Col + (Convert.ToInt32(groupByList.factTable[xAddressColID][YXMrow]) * numberColCount) - m;
                    targetRow = Convert.ToInt32(groupByList.factTable[yAddressColID][YXMrow]);
                    YXMcrosstabTable[targetCol][targetRow] = groupByList.factTable[xAddressColID - m][YXMrow];                   
                }
            }           

            // Add crosstab number header
            Dictionary<int, string> crosstabDataType = new Dictionary<int, string>();
            Dictionary<int, string> crosstabColumnName = new Dictionary<int, string>();
            Dictionary<string, int> crosstabupperColumnName2ID = new Dictionary<string, int>();
            List<string> crosstabNumberHeader = new List<string>();

            for (int x = 0; x < currentSetting.crosstabAggregateByColumnName.Count; x++)           
                if(currentSetting.crosstabAggregateFunction[x].ToUpper() == "COUNT" || currentSetting.crosstabAggregateFunction[x].ToUpper() == "SUM")
                    crosstabNumberHeader.Add(currentSetting.crosstabAggregateByColumnName[x]);
                else
                    crosstabNumberHeader.Add(currentSetting.crosstabAggregateFunction[x] + ":" + currentSetting.crosstabAggregateByColumnName[x]);           

            for (int x = 0; x < yDistinctList.dataType.Count; x++)
            {
                crosstabDataType.Add(x, yDistinctList.dataType[x]);              
                crosstabColumnName.Add(x, yDistinctList.columnName[x]);
                crosstabupperColumnName2ID.Add(yDistinctList.columnName[x].ToUpper(), x);
            }
           
            u = yColCount;
            int v = u;
            do
            {
                for (int i = 0; i < crosstabNumberHeader.Count; i++)
                {                   
                    crosstabDataType.Add(u, "Number");

                    if(crosstabNumberHeader[i].ToUpper() == "NULL")
                        crosstabColumnName.Add(u, v.ToString() + "@" + "Count");                  
                    else
                        crosstabColumnName.Add(u, v.ToString() + "@" + crosstabNumberHeader[i]);

                    crosstabupperColumnName2ID.Add(crosstabNumberHeader[i] + v.ToString() + "@" , u);
                    u++;
                }
                v++;

            } while (u < YXMcrosstabTable.Count);

            Dictionary<int, List<string>> crosstabHeader = new Dictionary<int, List<string>>();

            // Add x column name and data x data value
            for (int i = 0; i < xDistinctList.factTable.Count; i++)
            {
                crosstabHeader.Add(i, new List<string>());

                for (int j = 1; j < (xDistinctList.factTable[i].Count + yColCount); j++)
                    if (j < yColCount)
                        crosstabHeader[i].Add("");
                    else if (j == yColCount)
                        crosstabHeader[i].Add(xDistinctList.columnName[i]);
                    else
                        for (int k = 0; k < (crosstabNumberHeader.Count); k++)
                            crosstabHeader[i].Add(xDistinctList.key2Value[i][xDistinctList.factTable[i][j - yColCount]]);
                       
            }          

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = crosstabColumnName;
            currentOutput.upperColumnName2ID = crosstabupperColumnName2ID;
            currentOutput.dataType = crosstabDataType;
            currentOutput.factTable = YXMcrosstabTable;
            currentOutput.key2Value = yDistinctList.key2Value;
            currentOutput.value2Key = yDistinctList.value2Key;
            currentOutput.crosstabHeader = crosstabHeader;
            currentOutput.crosstabNumberHeader = crosstabNumberHeader;
            return currentOutput;
        }        
        public LedgerRAM groupBy(LedgerRAM currentTable, crosstabSetting currentSetting)
        {           
            List<string> groupByColumnName = new List<string>();
            List<string> aggregateFunction = new List<string>();
            List<string> aggregateByColumnName = new List<string>();

            groupBy newGroupBy = new groupBy();
            groupBySetting setGroupBy = new groupBySetting();

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                groupByColumnName.Add(currentSetting.yColumnName[x]);

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)
                groupByColumnName.Add(currentSetting.xColumnName[x]);

            setGroupBy.groupByColumnName = groupByColumnName;

            for (int x = 0; x < currentSetting.crosstabAggregateFunction.Count; x++)
                aggregateFunction.Add(currentSetting.crosstabAggregateFunction[x]);

            setGroupBy.aggregateFunction = aggregateFunction;

            for (int x = 0; x < currentSetting.crosstabAggregateByColumnName.Count; x++)
                aggregateByColumnName.Add(currentSetting.crosstabAggregateByColumnName[x]);

            setGroupBy.aggregateByColumnName = aggregateByColumnName;

            LedgerRAM resultGroupby = new LedgerRAM();

            if (currentSetting.command != "CROSSTABSQLTABLE")
                resultGroupby = newGroupBy.groupByList(currentTable, setGroupBy);

            else if (currentSetting.command == "CROSSTABSQLTABLE")
            {
                SQL newSQL = new SQL();
                string message = null;                
                SQLsetting setSQLsetting = new SQLsetting();
                setSQLsetting.currentSQLServer = currentSetting.currentSQLServer;
                setSQLsetting.currentConnectionString = currentSetting.currentConnectionString;
                setSQLsetting.resultTable = currentSetting.resultTable;
                setSQLsetting.groupByColumnName = groupByColumnName;
                setSQLsetting.aggregateFunction = aggregateFunction;
                setSQLsetting.aggregateByColumnName = aggregateByColumnName;
                setSQLsetting.selectedColumnName = groupByColumnName;
                setSQLsetting.sourceTable = currentSetting.sourceTable;

                /*
                for (int x = 0; x < groupByColumnName.Count; x++)
                    Console.WriteLine("groupByColumnName " + groupByColumnName[x]);

                for (int x = 0; x < aggregateFunction.Count; x++)
                    Console.WriteLine("aggregateFunction " + aggregateFunction[x]);

                for (int x = 0; x < aggregateByColumnName.Count; x++)
                    Console.WriteLine("aggregateByColumnName " + aggregateByColumnName[x]);

                Console.WriteLine(currentSetting.sourceTable + "  " + currentSetting.resultTable);
                */
                (resultGroupby, message) = newSQL.groupSQLTableBy(setSQLsetting);

                List<string> reOrderColumn = new List<string>();

                for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                    reOrderColumn.Add(currentSetting.yColumnName[x]);

                for (int x = 0; x < currentSetting.xColumnName.Count; x++)
                    reOrderColumn.Add(currentSetting.xColumnName[x]);

                for (int x = 0; x < currentSetting.crosstabAggregateByColumnName.Count; x++)
                    reOrderColumn.Add(currentSetting.crosstabAggregateByColumnName[x]);

                selectColumn newSelectColumn = new selectColumn();
                selectColumnSetting setSelectColumn = new selectColumnSetting();
                setSelectColumn.selectColumn = reOrderColumn;
                setSelectColumn.selectType = "Add";

                resultGroupby = newSelectColumn.selectColumnName(resultGroupby, setSelectColumn);
            }

            return resultGroupby;
        }
        public LedgerRAM groupByRowTotal(LedgerRAM currentTable, crosstabSetting currentSetting)
        {
            List<string> groupByColumnName = new List<string>();
            List<string> aggregateFunction = new List<string>();
            List<string> aggregateByColumnName = new List<string>();

            groupBy newGroupBy = new groupBy();
            groupBySetting setGroupBy = new groupBySetting();

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                groupByColumnName.Add(currentSetting.yColumnName[x]);           

            setGroupBy.groupByColumnName = groupByColumnName;

            for (int x = 0; x < currentSetting.crosstabAggregateFunction.Count; x++)
                aggregateFunction.Add(currentSetting.crosstabAggregateFunction[x]);

            setGroupBy.aggregateFunction = aggregateFunction;

            for (int x = 0; x < currentSetting.crosstabAggregateByColumnName.Count; x++)
                aggregateByColumnName.Add(currentSetting.crosstabAggregateByColumnName[x]);

            setGroupBy.aggregateByColumnName = aggregateByColumnName;

            Dictionary<string, string> orderByColumnNameY = new Dictionary<string, string>();
            orderBy newOrderBy = new orderBy();
            orderBySetting setOrderBy = new orderBySetting();

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                orderByColumnNameY.Add(groupByColumnName[x], "A");

            setOrderBy.orderByColumnName = orderByColumnNameY;

            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput = newOrderBy.orderByList(newGroupBy.groupByList(currentTable, setGroupBy), setOrderBy);

            return currentOutput;       
        }
        public LedgerRAM xDistinct(LedgerRAM currentTable, crosstabSetting currentSetting)
        {
            List<string> distinctColumnNameX = new List<string>();
            distinct newDistinct = new distinct();
            dinstinctSetting setDistinct = new dinstinctSetting();

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)
                distinctColumnNameX.Add(currentSetting.xColumnName[x]);

            setDistinct.distinctColumnName = distinctColumnNameX;           

            Dictionary<string, string> orderByColumnNameX = new Dictionary<string, string>();
            orderBy newOrderBy = new orderBy();
            orderBySetting setOrderBy = new orderBySetting();

            for (int x = 0; x < currentSetting.xColumnName.Count; x++)
                orderByColumnNameX.Add(currentSetting.xColumnName[x], "A");

            setOrderBy.orderByColumnName = orderByColumnNameX;
            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput = newOrderBy.orderByList(newDistinct.distinctList(currentTable, setDistinct), setOrderBy);
            return currentOutput;
        }        
        public LedgerRAM yDistinct(LedgerRAM currentTable, crosstabSetting currentSetting)
        {
            List<string> distinctColumnNameY = new List<string>();
            distinct newDistinct = new distinct();
            dinstinctSetting setDistinct = new dinstinctSetting();

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                distinctColumnNameY.Add(currentSetting.yColumnName[x]);

            setDistinct.distinctColumnName = distinctColumnNameY;           

            Dictionary<string, string> orderByColumnNameY = new Dictionary<string, string>();
            orderBy newOrderBy = new orderBy();
            orderBySetting setOrderBy = new orderBySetting();

            for (int x = 0; x < currentSetting.yColumnName.Count; x++)
                orderByColumnNameY.Add(currentSetting.yColumnName[x], "A");

            setOrderBy.orderByColumnName = orderByColumnNameY;
            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput = newOrderBy.orderByList(newDistinct.distinctList(currentTable, setDistinct), setOrderBy);
            return currentOutput;
        } 
    }
}
