using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class buildBalanceSetting
    {
        public int rowThread = 100;

        public int columnThread = 100;

        public bool isTransferBalance2SameAccount = true;      
        public List<string> columnName { get; set; }               
        public string buildBalanceType { get; set; }
    }
    
    public class buildBalance
    {
        public LedgerRAM buildBalanceByPeriod(LedgerRAM currentTable, LedgerRAM ledgerMaster, buildBalanceSetting currentSetting)
        {
            if (currentSetting.buildBalanceType == null)
                currentSetting.buildBalanceType = "BUILDMONTHLYBALANCE";

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<string> groupByColumnName = new List<string>();
            List<string> aggregateFunction = new List<string>();
            List<string> aggregateByColumnName = new List<string>();

            for (int x = 0; x < currentSetting.columnName.Count; x++)
            {
                if (currentTable.dataType[upperColumnName2ID[currentSetting.columnName[x].ToUpper()]] == "Number")
                { 
                    aggregateByColumnName.Add(currentSetting.columnName[x]);
                    aggregateFunction.Add("Sum");
                }
                else
                    groupByColumnName.Add(currentSetting.columnName[x]);
            }

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                groupByColumnName.Add("Period End");

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
                groupByColumnName.Add("dPeriod End");           

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                groupByColumnName.Add("wPeriod End");            

            List<string> upperColumnName = new List<string>();

            foreach (var pair in currentSetting.columnName)
                upperColumnName.Add(pair.ToUpper());           

            if (!upperColumnName.Contains("D/C"))
            {
                List<string> DC2PositiveNegative = new List<string>();

                foreach (var pair in currentSetting.columnName)               
                  if (currentTable.dataType[upperColumnName2ID[pair.ToUpper()]] == "Number")                   
                     DC2PositiveNegative.Add(pair);                   

                DC newDC2PositiveNegative = new DC();
                DCsetting setDC2PositiveNegative = new DCsetting();
                setDC2PositiveNegative.DC2PositiveNegative = DC2PositiveNegative;
                currentTable = newDC2PositiveNegative.DC2Number(currentTable, setDC2PositiveNegative);
            }

            groupBy newGroupBy = new groupBy();
            groupBySetting setGroupBy = new groupBySetting();
            setGroupBy.groupByColumnName = groupByColumnName;
            setGroupBy.aggregateFunction = aggregateFunction;
            setGroupBy.aggregateByColumnName = aggregateByColumnName;
            LedgerRAM currentOutput = newGroupBy.groupByList(currentTable, setGroupBy);
         
            currentOutput = buildBalanceProcess(currentOutput, ledgerMaster, currentSetting);

            List<string> leftJoinTableColumn = new List<string>();
            List<string> rightJoinTableColumn = new List<string>();

            for (int x = 0; x < ledgerMaster.columnName.Count; x++)
            {
                if(upperColumnName.Contains(ledgerMaster.columnName[x].ToUpper()))
                {
                    leftJoinTableColumn.Add(ledgerMaster.columnName[x]);
                    rightJoinTableColumn.Add(ledgerMaster.columnName[x]);                  
                }
            }

            conditionalJoin newConditionalJoin = new conditionalJoin();
            conditionalJoinSetting setConditionalJoin = new conditionalJoinSetting();                      
            setConditionalJoin.leftTableColumn = leftJoinTableColumn;
            setConditionalJoin.rightTableColumn = rightJoinTableColumn;
            setConditionalJoin.joinTableType = "conditionalJoin";          

            currentOutput = newConditionalJoin.conditionalJoinProcess(currentOutput, ledgerMaster, setConditionalJoin);        

            List<string> yColumnName = new List<string>();

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
            { 
                for (int x = 0; x < currentOutput.columnName.Count; x++)
                {
                    if (currentOutput.columnName[x].ToUpper() != "PERIOD END" && currentOutput.columnName[x].ToUpper() != "PERIOD CHANGE")
                    {
                        if (!currentOutput.columnName[x].ToUpper().Contains("YEAR END"))
                        {
                            if (currentOutput.dataType[x] != "Number")
                                yColumnName.Add(currentOutput.columnName[x]);                     
                        }
                    }
                }
            }

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
            {
                for (int x = 0; x < currentOutput.columnName.Count; x++)
                {
                    if (currentOutput.columnName[x].ToUpper() != "DPERIOD END" && currentOutput.columnName[x].ToUpper() != "DPERIOD CHANGE")
                    {
                        if (!currentOutput.columnName[x].ToUpper().Contains("YEAR END"))
                        {
                            if (currentOutput.dataType[x] != "Number")
                                yColumnName.Add(currentOutput.columnName[x]);
                        }
                    }
                }
            }


            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
            {
                for (int x = 0; x < currentOutput.columnName.Count; x++)
                {
                    if (currentOutput.columnName[x].ToUpper() != "WPERIOD END" && currentOutput.columnName[x].ToUpper() != "WPERIOD CHANGE")
                    {
                        if (!currentOutput.columnName[x].ToUpper().Contains("YEAR END"))
                        {
                            if (currentOutput.dataType[x] != "Number")
                                yColumnName.Add(currentOutput.columnName[x]);
                        }
                    }
                }
            }


            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE")
            {
                yColumnName.Add("Period End");

                setGroupBy.groupByColumnName = yColumnName;
                setGroupBy.aggregateFunction = aggregateFunction;
                setGroupBy.aggregateByColumnName = aggregateByColumnName;
                currentOutput = newGroupBy.groupByList(currentOutput, setGroupBy);
            }

            if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE")
            {
                yColumnName.Add("dPeriod End");

                setGroupBy.groupByColumnName = yColumnName;
                setGroupBy.aggregateFunction = aggregateFunction;
                setGroupBy.aggregateByColumnName = aggregateByColumnName;
                currentOutput = newGroupBy.groupByList(currentOutput, setGroupBy);
            }

            if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE")
            {
                yColumnName.Add("wPeriod End");

                setGroupBy.groupByColumnName = yColumnName;
                setGroupBy.aggregateFunction = aggregateFunction;
                setGroupBy.aggregateByColumnName = aggregateByColumnName;
                currentOutput = newGroupBy.groupByList(currentOutput, setGroupBy);
            }

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
            {              
                List<string> crosstabAggregateFunction = new List<string>();
                List<string> crosstabAggregateByColumnName = new List<string>();
                List<string> xColumnName = new List<string>();
                xColumnName.Add("Period End");

                crosstab newCrosstab = new crosstab();
                crosstabSetting setCrosstab = new crosstabSetting();
                setCrosstab.xColumnName = xColumnName;
                setCrosstab.yColumnName = yColumnName;
                setCrosstab.crosstabAggregateFunction = aggregateFunction;
                setCrosstab.crosstabAggregateByColumnName = aggregateByColumnName;
                currentOutput = newCrosstab.crosstabTable(currentOutput, setCrosstab);
            }

            if (currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
            {
                List<string> crosstabAggregateFunction = new List<string>();
                List<string> crosstabAggregateByColumnName = new List<string>();
                List<string> xColumnName = new List<string>();
                xColumnName.Add("dPeriod End");

                crosstab newCrosstab = new crosstab();
                crosstabSetting setCrosstab = new crosstabSetting();
                setCrosstab.xColumnName = xColumnName;
                setCrosstab.yColumnName = yColumnName;
                setCrosstab.crosstabAggregateFunction = aggregateFunction;
                setCrosstab.crosstabAggregateByColumnName = aggregateByColumnName;
                currentOutput = newCrosstab.crosstabTable(currentOutput, setCrosstab);
            }

            if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
            {
                List<string> crosstabAggregateFunction = new List<string>();
                List<string> crosstabAggregateByColumnName = new List<string>();
                List<string> xColumnName = new List<string>();
                xColumnName.Add("wPeriod End");

                crosstab newCrosstab = new crosstab();
                crosstabSetting setCrosstab = new crosstabSetting();
                setCrosstab.xColumnName = xColumnName;
                setCrosstab.yColumnName = yColumnName;
                setCrosstab.crosstabAggregateFunction = aggregateFunction;
                setCrosstab.crosstabAggregateByColumnName = aggregateByColumnName;
                currentOutput = newCrosstab.crosstabTable(currentOutput, setCrosstab);
            }

            return currentOutput;
        }
        public LedgerRAM buildBalanceProcess(LedgerRAM currentTable, LedgerRAM yearEndAccountTable, buildBalanceSetting currentSetting)
        {
            if (currentSetting.buildBalanceType == null)
                currentSetting.buildBalanceType = "BUILDMONTHLYBALANCE";

            LedgerRAM allAccountBalanceTable = new LedgerRAM();
            
            if (yearEndAccountTable != null)
            {
                List<string> balanceTypeColumnName = new List<string>();
                List<int> balanceTypeColumnID = new List<int>();
                List<int> balanceTypeColumnIDreorder = new List<int>();
                List<string> balanceTypeColumnNameReorder = new List<string>();
                List<int> joinColumnID = new List<int>();
                List<int> yearEndColumnID = new List<int>();
                List<int> yearEndColumnIDreorder = new List<int>();
                List<string> yearEndColumnNameReorder = new List<string>();
                List<string> yearEndColumnName = new List<string>();

                Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

                foreach (var pair in currentTable.columnName)
                    upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

                for (int x = 0; x < yearEndAccountTable.columnName.Count; x++)
                {
                    if (yearEndAccountTable.columnName[x].Contains(":"))
                    {
                        string col = yearEndAccountTable.columnName[x].Substring(yearEndAccountTable.columnName[x].IndexOf(":") + 1, yearEndAccountTable.columnName[x].Length - yearEndAccountTable.columnName[x].IndexOf(":") - 1);
                        yearEndColumnName.Add(col.ToUpper());
                    }
                }

                for (int x = 0; x < yearEndAccountTable.columnName.Count; x++)
                {
                    if (yearEndColumnName.Contains(yearEndAccountTable.columnName[x].ToUpper()) || upperColumnName2ID.ContainsKey(yearEndAccountTable.columnName[x].ToUpper()))
                    {
                        balanceTypeColumnName.Add(yearEndAccountTable.columnName[x]);
                        balanceTypeColumnID.Add(x);
                        //Console.WriteLine("balanceTypeColumnID " + x);
                    }
                    else if (yearEndAccountTable.columnName[x].ToUpper().Contains("YEAR END"))
                    {
                        yearEndColumnID.Add(x);
                        //Console.WriteLine("yearEndColumnID " + x);
                    }
                    else
                    {
                        //Console.WriteLine("joinColumnID " + x);
                        joinColumnID.Add(x);
                    }
                }

                for (int i = 0; i < yearEndColumnID.Count; i++)
                {
                    for (int j = 0; j < balanceTypeColumnID.Count; j++)
                    {
                        if (yearEndAccountTable.columnName[yearEndColumnID[i]].ToUpper().Contains(yearEndAccountTable.columnName[balanceTypeColumnID[j]].ToUpper()))
                        {                          
                            balanceTypeColumnIDreorder.Add(balanceTypeColumnID[j]);
                            balanceTypeColumnNameReorder.Add(yearEndAccountTable.columnName[balanceTypeColumnID[j]].ToUpper());
                            yearEndColumnIDreorder.Add(yearEndColumnID[i]);
                            yearEndColumnNameReorder.Add(yearEndAccountTable.columnName[yearEndColumnID[i]]);
                        }
                    }
                }

                Dictionary<string, Dictionary<string, string>> PL2RetainedAccount = new Dictionary<string, Dictionary<string, string>>();
                currentSetting.isTransferBalance2SameAccount = false;

                for (int x = 0; x < balanceTypeColumnIDreorder.Count; x++)
                {
                    var currentColumnName = yearEndAccountTable.columnName[balanceTypeColumnIDreorder[x]].ToUpper();
                    PL2RetainedAccount.Add(currentColumnName, new Dictionary<string, string>());

                    for (int y = 1; y < yearEndAccountTable.factTable[0].Count; y++)
                    {
                        string retainAC = yearEndAccountTable.key2Value[yearEndColumnIDreorder[x]][yearEndAccountTable.factTable[yearEndColumnIDreorder[x]][y]];

                        string currentKey = yearEndAccountTable.key2Value[balanceTypeColumnIDreorder[x]][yearEndAccountTable.factTable[balanceTypeColumnIDreorder[x]][y]];

                        if (!PL2RetainedAccount[currentColumnName].ContainsKey(currentKey))
                            PL2RetainedAccount[currentColumnName].Add(currentKey, retainAC);
                    }
                }

                selectColumn newSelectColumn = new selectColumn();
                selectColumnSetting setSelectColumn = new selectColumnSetting();
                setSelectColumn.selectColumn = balanceTypeColumnNameReorder;
                LedgerRAM _yearEndAccountTable = newSelectColumn.selectColumnName(yearEndAccountTable, setSelectColumn);
                filter newFilterByConditionList = new filter();
                filterSetting setFilterByConditionList = new filterSetting();
                setFilterByConditionList.filterType = "And";
                currentTable = newFilterByConditionList.filterByConditionList(currentTable, _yearEndAccountTable, setFilterByConditionList);               

                allAccountBalanceTable = buildDifferentBalance(balanceTypeColumnNameReorder, PL2RetainedAccount, yearEndAccountTable, currentTable, currentSetting);               
            }
            else
                allAccountBalanceTable = buildDifferentBalance(null, null, yearEndAccountTable, currentTable, currentSetting);

            return allAccountBalanceTable;
        }
        public LedgerRAM buildDifferentBalance(List<string> balanceTypeColumnNameReorder, Dictionary<string, Dictionary<string, string>> PL2RetainedAccount, LedgerRAM yearEndAccountTable, LedgerRAM currentTable, buildBalanceSetting currentSetting)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            int periodColumnID = 0;

            foreach (var pair in currentTable.columnName)
                if (!upperColumnName2ID.ContainsKey(pair.Value.ToUpper()))
                    upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);               

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                periodColumnID = upperColumnName2ID["PERIOD END"];

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
                periodColumnID = upperColumnName2ID["DPERIOD END"];

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                periodColumnID = upperColumnName2ID["WPERIOD END"];

            // Distinct one column "Period End"
            List<string> distinctColumnName = new List<string>();

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                distinctColumnName.Add("Period End");

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
                distinctColumnName.Add("dPeriod End");

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                distinctColumnName.Add("wPeriod End");

            distinct newDistinct = new distinct();
            dinstinctSetting setDistinct = new dinstinctSetting();
            setDistinct.distinctColumnName = distinctColumnName;

            LedgerRAM periodRange = newDistinct.distinctList(currentTable, setDistinct);

            // OrderBy one column "Period End"           
            Dictionary<string, string> orderByColumnName = new Dictionary<string, string>();

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                orderByColumnName.Add("Period End", "A");

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
                orderByColumnName.Add("dPeriod End", "A");

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                orderByColumnName.Add("wPeriod End", "A");

            orderBy newOrderBy = new orderBy();
            orderBySetting setOrderBy = new orderBySetting();
            setOrderBy.orderByColumnName = orderByColumnName;
            periodRange = newOrderBy.orderByList(periodRange, setOrderBy);

            string startPeriod = periodRange.key2Value[0][periodRange.factTable[0][1]];
            string endPeriod = periodRange.key2Value[0][periodRange.factTable[0][periodRange.factTable[0].Count - 1]];           
         
            bool success = Int32.TryParse(startPeriod.Substring(0, 4), out int startYear);
            success = Int32.TryParse(endPeriod.Substring(0, 4), out int endYear);

            Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodMovement = new Dictionary<double, Dictionary<int, List<double>>>();
            Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodBalance = new Dictionary<double, Dictionary<int, List<double>>>();         

            List<string> selectedColumnName = new List<string>();
            Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                selectedColumnName.Add("Period End");

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
                selectedColumnName.Add("dPeriod End");

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                selectedColumnName.Add("wPeriod End");

            compareOperator.Add(0, new List<string>());
            compareOperator[0].Add("=");

            selectedTextNumber.Add(0, new List<string>());

            List<string> period = new List<string>();
            LedgerRAM periodMovement = new LedgerRAM();
            string currentPeriod;           

            Dictionary<double, string> periodTableKey2Value = new Dictionary<double, string>(periodRange.key2Value[0]);
            Dictionary<string, double> periodTableValue2Key = new Dictionary<string, double>(periodRange.value2Key[0]);

            int count;

            int h = 0;

           // Console.WriteLine();
           // Console.WriteLine(startYear + "  " + endYear);

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
            {
                for (int i = startYear; i <= endYear; i++)
                {
                    for (int j = 1; j <= 12; j++)
                    {
                        if (j >= 10)
                            currentPeriod = i + "p" + j;
                        else
                            currentPeriod = i + "p0" + j;

                        if (string.Compare(startPeriod.ToString(), currentPeriod.ToString()) <= 0 && string.Compare(endPeriod.ToString(), currentPeriod.ToString()) >= 0)
                        {
                            h++;

                            period.Add(currentPeriod);
                            periodTableKey();

                            if (j == 12)
                            {
                                currentPeriod = "!" + currentPeriod;
                                period.Add(currentPeriod);
                                periodTableKey();
                            }

                        }

                        void periodTableKey()
                        {
                            if (!periodTableValue2Key.ContainsKey(currentPeriod))
                            {
                                count = periodTableValue2Key.Count;
                                periodTableKey2Value.Add(count, currentPeriod);
                                periodTableValue2Key.Add(currentPeriod, count);
                            }

                            groupbyPeriodMovement.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());
                            groupbyPeriodBalance.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());

                            for (int x = 0; x < currentTable.factTable.Count; x++)
                            {
                                groupbyPeriodMovement[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                                groupbyPeriodBalance[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                            }
                        }
                    }
                }
            }

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
            {
                for (int i = startYear; i <= endYear; i++)
                {
                    for (int j = 1; j <= 366; j++)
                    {
                        if (j >= 100)
                            currentPeriod = i + "p" + j;
                        else if(j >= 10)
                            currentPeriod = i + "p0" + j;
                        else
                            currentPeriod = i + "p00" + j;

                        if (string.Compare(startPeriod.ToString(), currentPeriod.ToString()) <= 0 && string.Compare(endPeriod.ToString(), currentPeriod.ToString()) >= 0)
                        {
                            h++;

                            period.Add(currentPeriod);
                            periodTableKey();

                            if (j == 366)
                            {
                                currentPeriod = "!" + currentPeriod;
                                period.Add(currentPeriod);
                                periodTableKey();
                            }
                        }

                        void periodTableKey()
                        {
                            if (!periodTableValue2Key.ContainsKey(currentPeriod))
                            {
                                count = periodTableValue2Key.Count;
                                periodTableKey2Value.Add(count, currentPeriod);
                                periodTableValue2Key.Add(currentPeriod, count);
                            }

                            groupbyPeriodMovement.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());
                            groupbyPeriodBalance.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());

                            for (int x = 0; x < currentTable.factTable.Count; x++)
                            {
                                groupbyPeriodMovement[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                                groupbyPeriodBalance[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                            }
                        }
                    }
                }
            }

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
            {
                for (int i = startYear; i <= endYear; i++)
                {
                    for (int j = 1; j <= 53; j++)
                    {
                        if (j >= 10)
                            currentPeriod = i + "p" + j;
                        else
                            currentPeriod = i + "p0" + j;

                        if (string.Compare(startPeriod.ToString(), currentPeriod.ToString()) <= 0 && string.Compare(endPeriod.ToString(), currentPeriod.ToString()) >= 0)
                        {
                            h++;

                            period.Add(currentPeriod);
                            periodTableKey();

                            if (j == 53)
                            {
                                currentPeriod = "!" + currentPeriod;
                                period.Add(currentPeriod);
                                periodTableKey();
                            }
                        }

                        void periodTableKey()
                        {
                            if (!periodTableValue2Key.ContainsKey(currentPeriod))
                            {
                                count = periodTableValue2Key.Count;
                                periodTableKey2Value.Add(count, currentPeriod);
                                periodTableValue2Key.Add(currentPeriod, count);
                            }

                            groupbyPeriodMovement.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());
                            groupbyPeriodBalance.Add(periodTableValue2Key[currentPeriod], new Dictionary<int, List<double>>());

                            for (int x = 0; x < currentTable.factTable.Count; x++)
                            {
                                groupbyPeriodMovement[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                                groupbyPeriodBalance[periodTableValue2Key[currentPeriod]].Add(x, new List<double>());
                            }
                        }
                    }
                }
            }

            ConcurrentDictionary<int, buildBalance> writeColumnThread = new ConcurrentDictionary<int, buildBalance>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();       

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, currentTable.factTable.Count, options, x =>          
            {
                groupbyPeriodMovementOneColumn(x, checkThreadCompleted, periodColumnID, groupbyPeriodMovement, currentTable);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < currentTable.factTable.Count);

            options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, currentTable.factTable.Count, options, x =>
            {
                groupbyPeriodBalanceOneColumn(currentTable, balanceTypeColumnNameReorder, PL2RetainedAccount, x, checkThreadCompleted, period, groupbyPeriodMovement, groupbyPeriodBalance, periodTableKey2Value, periodTableValue2Key, currentSetting);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < currentTable.factTable.Count);

            Dictionary<int, List<double>> balanceFactTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < currentTable.factTable.Count + 1; x++)          
                balanceFactTable.Add(x, new List<double>());           

            options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, currentTable.factTable.Count + 1, options, x =>
            {
                balanceFactTable[x] = balanceTableOneColumn(x, checkThreadCompleted, period, groupbyPeriodBalance, periodTableValue2Key);
            });            

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < currentTable.factTable.Count + 1);
           
            Dictionary<int, string> resultdataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            if (currentSetting.buildBalanceType == "BUILDMONTHLYBALANCE" || currentSetting.buildBalanceType == "BUILDMONTHLYBALANCECROSSTABPERIOD")
            {
                resultdataType.Add(0, "Period");
                resultColumnName.Add(0, "Period End");
                resultKey2Value.Add(0, periodTableKey2Value);
                resultValue2Key.Add(0, periodTableValue2Key);

                for (int x = 1; x < currentTable.factTable.Count + 1; x++)
                {
                    resultdataType.Add(x, currentTable.dataType[x - 1]);

                    if (currentTable.columnName[x - 1].ToUpper() == "PERIOD END")
                        resultColumnName.Add(x, "Period Change");
                    else
                        resultColumnName.Add(x, currentTable.columnName[x - 1]);

                    if (currentTable.dataType[x - 1] != "Number")
                    {
                        resultKey2Value.Add(x, currentTable.key2Value[x - 1]);
                        resultValue2Key.Add(x, currentTable.value2Key[x - 1]);
                    }
                }
            }

            else if (currentSetting.buildBalanceType == "BUILDDAILYBALANCE" || currentSetting.buildBalanceType == "BUILDDAILYBALANCECROSSTABPERIOD")
            {
                resultdataType.Add(0, "dPeriod");
                resultColumnName.Add(0, "dPeriod End");
                resultKey2Value.Add(0, periodTableKey2Value);
                resultValue2Key.Add(0, periodTableValue2Key);

                for (int x = 1; x < currentTable.factTable.Count + 1; x++)
                {
                    resultdataType.Add(x, currentTable.dataType[x - 1]);

                    if (currentTable.columnName[x - 1].ToUpper() == "DPERIOD END")
                        resultColumnName.Add(x, "dPeriod Change");
                    else
                        resultColumnName.Add(x, currentTable.columnName[x - 1]);

                    if (currentTable.dataType[x - 1] != "Number")
                    {
                        resultKey2Value.Add(x, currentTable.key2Value[x - 1]);
                        resultValue2Key.Add(x, currentTable.value2Key[x - 1]);
                    }
                }
            }

            else if (currentSetting.buildBalanceType == "BUILDWEEKLYBALANCE" || currentSetting.buildBalanceType == "BUILDWEEKLYBALANCECROSSTABPERIOD")
            {
                resultdataType.Add(0, "wPeriod");
                resultColumnName.Add(0, "wPeriod End");
                resultKey2Value.Add(0, periodTableKey2Value);
                resultValue2Key.Add(0, periodTableValue2Key);

                for (int x = 1; x < currentTable.factTable.Count + 1; x++)
                {
                    resultdataType.Add(x, currentTable.dataType[x - 1]);

                    if (currentTable.columnName[x - 1].ToUpper() == "WPERIOD END")
                        resultColumnName.Add(x, "wPeriod Change");
                    else
                        resultColumnName.Add(x, currentTable.columnName[x - 1]);

                    if (currentTable.dataType[x - 1] != "Number")
                    {
                        resultKey2Value.Add(x, currentTable.key2Value[x - 1]);
                        resultValue2Key.Add(x, currentTable.value2Key[x - 1]);
                    }
                }
            }

            for (int x = 0; x < currentTable.factTable.Count + 1; x++)
                resultUpperColumnName2ID.Add(resultColumnName[x].ToUpper(), x);

            LedgerRAM currentOutput = new LedgerRAM();
            
            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultdataType;
            currentOutput.factTable = balanceFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;
            currentOutput.isPeriodEndExist = currentTable.isPeriodEndExist;
           
            return currentOutput;
        }       
        public void groupbyPeriodMovementOneColumn(int x, ConcurrentQueue<int> checkThreadCompleted, int periodColumnID, Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodMovement, LedgerRAM currentTable)
        {            
            for (int y = 1; y < currentTable.factTable[0].Count; y++)            
                groupbyPeriodMovement[currentTable.factTable[periodColumnID][y]][x].Add(currentTable.factTable[x][y]);
            
            checkThreadCompleted.Enqueue(x);
        }
        public void groupbyPeriodBalanceOneColumn(LedgerRAM currentTable, List<string> balanceTypeColumnNameReorder, Dictionary<string, Dictionary<string, string>> PL2RetainedAccount, int x, ConcurrentQueue<int> checkThreadCompleted, List<string> period, Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodMovement, Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodBalance, Dictionary<double, string> periodTableKey2Value, Dictionary<string, double> periodTableValue2Key, buildBalanceSetting currentSetting)
        {
            groupbyPeriodBalance[0][x].AddRange(groupbyPeriodMovement[periodTableValue2Key[period[0]]][x]);
            int count;
            for (int p = 1; p < period.Count; p++)
            {  
                groupbyPeriodBalance[periodTableValue2Key[period[p]]][x].AddRange(groupbyPeriodBalance[periodTableValue2Key[period[p - 1]]][x]);

                if (groupbyPeriodMovement[periodTableValue2Key[period[p]]][x].Count > 0)
                    groupbyPeriodBalance[periodTableValue2Key[period[p]]][x].AddRange(groupbyPeriodMovement[periodTableValue2Key[period[p]]][x]);               

                if (currentSetting.isTransferBalance2SameAccount == false)
                {   
                    if (periodTableKey2Value[periodTableValue2Key[period[p]]].Contains("!"))
                    {
                        if (balanceTypeColumnNameReorder.Contains(currentTable.columnName[x].ToUpper()))
                        {
                            for (int y = 0; y < groupbyPeriodBalance[periodTableValue2Key[period[p]]][x].Count; y++)
                            {
                                var currentValue = currentTable.key2Value[x][groupbyPeriodBalance[periodTableValue2Key[period[p]]][x][y]];                              

                                var currentText = PL2RetainedAccount[currentTable.columnName[x].ToUpper()][currentValue];

                                if (!currentTable.value2Key[x].ContainsKey(currentText))
                                {
                                     count = currentTable.value2Key[x].Count;
                                     currentTable.key2Value[x].Add(count, currentText);
                                     currentTable.value2Key[x].Add(currentText, count);                                     
                                }
                                else
                                    groupbyPeriodBalance[periodTableValue2Key[period[p]]][x][y] = currentTable.value2Key[x][currentText];
                               
                            }
                        }
                    }                   
                }  
            }           

            checkThreadCompleted.Enqueue(x);
        }

        public List<double> balanceTableOneColumn(int x, ConcurrentQueue<int> checkThreadCompleted, List<string> period, Dictionary<double, Dictionary<int, List<double>>> groupbyPeriodBalance, Dictionary<string, double> periodTableValue2Key)
        {
            List<double> balanceFactTable = new List<double>();
            balanceFactTable.Add(x);

            if (x == 0)
            {
                for (int p = 0; p < period.Count; p++)
                    if (groupbyPeriodBalance.ContainsKey(p))
                        for (int y = 0; y < groupbyPeriodBalance[p][0].Count; y++)
                            balanceFactTable.Add(p);
            }
            else if (x > 0)
            {
                for (int p = 0; p < period.Count; p++)
                    if(groupbyPeriodBalance.ContainsKey(p))
                        if(groupbyPeriodBalance[p].ContainsKey(x-1))
                            if(groupbyPeriodBalance[p][x - 1].Count > 0)
                                balanceFactTable.AddRange(groupbyPeriodBalance[p][x - 1]);
            }

            checkThreadCompleted.Enqueue(x);

            return balanceFactTable;
        }
    }
}

