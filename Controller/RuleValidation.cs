using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MaxAccountExtension;

namespace MaxAccount
{
    public class ruleProcessorSetting
    {
        public string separator = ",";
        public int rowThread = 100;
        public string calcRule { get; set; }
    }

    public class ruleProcessing
    {
        public bool processRule(ruleProcessorSetting currentSetting, List<string> newRule, ConcurrentQueue<string> processMessage)
        {
            DateTime startTime = DateTime.Now;
            string message = null;
            string classMessage = null;

            bool isExecute = true;
            bool isProcessEnd = false;

            Dictionary<string, LedgerRAM> ramStore = new Dictionary<string, LedgerRAM>();
            LedgerRAM currentProcess = new LedgerRAM(); // new one instance of the in-memory "LedgerRAM" processing                                  

            byte[] ruleBytestream = File.ReadAllBytes(currentSetting.calcRule);
            Dictionary<string, Dictionary<int, StringBuilder>> ruleDetail = new Dictionary<string, Dictionary<int, StringBuilder>>();
            
            StringBuilder currentDeclaredBlockName = new StringBuilder();
            int at = 0;
            int openCloseCurlyBracket = 0;
            int openCurlyBracket = 0;
            int closeCurlyBracket = 0;            
            int remark = 0;
            int declareblock = 0;
            int multiRowRemark = 0;           
            int u = 0;
            string currentBlock = "Main";       
            int line = 1;         

            // Initial Read Rule, save as ruleDetail
            for (int i = 0; i < ruleBytestream.Length; i++)
            {
                if (ruleBytestream[i] == 123)
                {
                    /*
                    if (openCloseCurlyBracket == 1)
                    {
                        message = "Rule " + line + " has duplicated { " + Environment.NewLine; Console.Write(message); File.AppendAllText("Output\\log.txt", message);
                        isExecute = false;
                        break;
                    }
                    else
                    */
                    {
                        openCloseCurlyBracket++;
                        openCurlyBracket++;
                    }
                }

                if (i < ruleBytestream.Length - 1)
                {
                    if (ruleBytestream[i] == 47 && ruleBytestream[i + 1] == 47)
                    {                      
                        remark++;
                        do
                        {
                            i++;

                        } while (ruleBytestream[i] != 13);
                    }

                    if (ruleBytestream[i] == 47 && ruleBytestream[i + 1] == 42)
                    {
                        multiRowRemark++;

                        do
                        {
                            i++;

                        } while (ruleBytestream[i] != 42 && ruleBytestream[i + 1] != 47);
                    }
                }
              
                if (ruleBytestream[i] == 35)                
                    declareblock++;

                if (declareblock != 0)
                {
                    if (ruleBytestream[i] != 10 && ruleBytestream[i] != 13)
                    {                       
                        if (ruleBytestream[i] != 35)
                            currentDeclaredBlockName.Append((char)ruleBytestream[i]);
                    }
                    else
                    {
                        declareblock = 0;
                        currentBlock = currentDeclaredBlockName.ToString().Trim();                       
                        u = 0;
                        currentDeclaredBlockName.Clear();
                    }
                }
                else
                {
                    if (ruleBytestream[i] == 10 || ruleBytestream[i] == 13)                    
                        remark = 0;
                }

                if (declareblock == 0 && remark == 0 && multiRowRemark == 0 && ruleBytestream[i] != 10 && ruleBytestream[i] != 13)
                {
                    if (openCloseCurlyBracket == 0 && ruleBytestream[i] != 32 && ruleBytestream[i] != 123)
                    {
                        if (!ruleDetail.ContainsKey(currentBlock))
                            ruleDetail.Add(currentBlock, new Dictionary<int, StringBuilder>());

                        if (!ruleDetail[currentBlock].ContainsKey(u))
                            ruleDetail[currentBlock].Add(u, new StringBuilder());

                        ruleDetail[currentBlock][u].Append((char)ruleBytestream[i]);
                    }

                    if (openCloseCurlyBracket == 1)
                    {
                        if (!ruleDetail.ContainsKey(currentBlock))
                            ruleDetail.Add(currentBlock, new Dictionary<int, StringBuilder>());

                        if (!ruleDetail[currentBlock].ContainsKey(u))
                            ruleDetail[currentBlock].Add(u, new StringBuilder());

                        ruleDetail[currentBlock][u].Append((char)ruleBytestream[i]);
                    }
                }

                if (i < ruleBytestream.Length - 1)
                {
                    if (ruleBytestream[i] == 42 && ruleBytestream[i + 1] == 47)
                        multiRowRemark = 0;
                }

                if ((remark == 0 && multiRowRemark == 0 && ruleBytestream[i] == 125))
                {
                   
                    openCloseCurlyBracket--;
                    closeCurlyBracket++;
                    u++;
                    declareblock = 0;                    

                    if (closeCurlyBracket > openCurlyBracket)
                    {
                        message = "Rule " + (line - 1) + " does not have correct {}" + Environment.NewLine;
                        processMessage.Enqueue(message);                                              
                        isExecute = false;
                        break;
                    }

                    line++;
                }
            } 
           
            Dictionary<string, Dictionary<int, List<string>>> ruleTypeParameter = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> afterRuleTypeParameter = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, string> arrowRuleTypeDict = new Dictionary<string, string>();
            arrowRuleTypeDict.Add("COMPUTECOLUMN", "ComputeColumn");
            arrowRuleTypeDict.Add("COMPUTECELL", "ComputeCell");
            arrowRuleTypeDict.Add("TABLE2CELL", "Table2Cell");
            arrowRuleTypeDict.Add("NUMBER2TEXT", "Number2Text");
            arrowRuleTypeDict.Add("DISTINCT", "Distinct");
            arrowRuleTypeDict.Add("REVERSENUMBER", "ReverseNumber");
            arrowRuleTypeDict.Add("COMBINETABLEBYCOMMONCOLUMN", "CombineTableByCommonColumn");
            arrowRuleTypeDict.Add("MERGECOMMONTABLE", "MergeCommonTable");
            arrowRuleTypeDict.Add("MERGETABLE", "MergeTable");
            arrowRuleTypeDict.Add("REVERSEDC", "ReverseDC");
            arrowRuleTypeDict.Add("SELECTCOLUMN", "SelectColumn");
            arrowRuleTypeDict.Add("REMOVECOLUMN", "RemoveColumn");
            arrowRuleTypeDict.Add("DC2POSITIVENEGATIVE", "DC2PositiveNegative");
            arrowRuleTypeDict.Add("DC2NEGATIVEPOSITIVE", "DC2NegativePositive");
            arrowRuleTypeDict.Add("POSITIVENEGATIVE2DC", "PositiveNegative2DC");
            arrowRuleTypeDict.Add("NEGATIVEPOSITIVE2DC", "NegativePositive2DC");
            arrowRuleTypeDict.Add("CSV2LEDGERRAM", "CSV2LEDGERRAM");
            arrowRuleTypeDict.Add("TEXT2LEDGERRAM", "Text2LedgerRAM");
            arrowRuleTypeDict.Add("ONECOLUMN2LEDGERRAM", "OneColumn2LedgerRAM");
            arrowRuleTypeDict.Add("REVERSECROSSTABCSV2LEDGERRAM", "ReverseCrosstabCSV2LedgerRAM");
            arrowRuleTypeDict.Add("CSV2DATATABLE", "CSV2DataTable");
            arrowRuleTypeDict.Add("CSV2HTML", "CSV2HTML");
            arrowRuleTypeDict.Add("CSV2JSON", "CSV2JSON");
            arrowRuleTypeDict.Add("CSV2XML", "CSV2XML");
            arrowRuleTypeDict.Add("DATATABLE2LEDGERRAM", "DataTable2LEDGERRAM");
            arrowRuleTypeDict.Add("DATATABLE2CSV", "DataTable2CSV");
            arrowRuleTypeDict.Add("DATATABLE2HTML", "DataTable2HTML");
            arrowRuleTypeDict.Add("DATATABLE2JSON", "DataTable2JSON");
            arrowRuleTypeDict.Add("DATATABLE2XML", "DataTable2XML");
            arrowRuleTypeDict.Add("LEDGERRAM2CSV", "LEDGERRAM2CSV");            
            arrowRuleTypeDict.Add("LEDGERRAM2TEXT", "LEDGERRAM2TEXT");
            arrowRuleTypeDict.Add("LEDGERRAM2ONECOLUMN", "LEDGERRAM2ONECOLUMN");
            arrowRuleTypeDict.Add("LEDGERRAM2DATATABLE", "LEDGERRAM2DataTable");
            arrowRuleTypeDict.Add("LEDGERRAM2HTML", "LEDGERRAM2HTML");
            arrowRuleTypeDict.Add("LEDGERRAM2JSON", "LEDGERRAM2JSON");
            arrowRuleTypeDict.Add("LEDGERRAM2XML", "LEDGERRAM2XML");
            arrowRuleTypeDict.Add("ENDPROCESS", "ENDPROCESS");
            arrowRuleTypeDict.Add("DISABLE", "Disable");
            arrowRuleTypeDict.Add("ENABLE", "Enable");
            arrowRuleTypeDict.Add("DATE2EFFECTIVEDATE", "Date2EffectiveDate");
            arrowRuleTypeDict.Add("COPYTABLE", "CopyTable");                
            arrowRuleTypeDict.Add("PROCESS", "Process");
            arrowRuleTypeDict.Add("CONTINUEPROCESS", "ContinueProcess");
            arrowRuleTypeDict.Add("CURRENTTABLE", "CurrentTable");
            arrowRuleTypeDict.Add("CURRENTBUILDBALANCESETTING", "CurrentBuildBalanceSetting");
            arrowRuleTypeDict.Add("ANDFILTER.DISTINCTLIST", "AndFilter.DistinctList");
            arrowRuleTypeDict.Add("ORFILTER.DISTINCTLIST", "OrFilter.DistinctList");
            arrowRuleTypeDict.Add("ANDFILTER.CONDITIONLIST", "AndFilter.ConditionList");
            arrowRuleTypeDict.Add("ORFILTER.CONDITIONLIST", "OrFilter.ConditionList");
            arrowRuleTypeDict.Add("BUILDMONTHLYBALANCECROSSTABPERIOD", "BuildMonthlyBalanceCrosstabPeriod");
            arrowRuleTypeDict.Add("BUILDMONTHLYBALANCE", "BuildMonthlyBalance");
            arrowRuleTypeDict.Add("BUILDWEEKLYBALANCECROSSTABPERIOD", "BuildWeeklyBalanceCrosstabPeriod");
            arrowRuleTypeDict.Add("BUILDWEEKLYBALANCE", "BuildWeeklyBalance");
            arrowRuleTypeDict.Add("BUILDDAILYBALANCECROSSTABPERIOD", "BuildDailyBalanceCrosstabPeriod");
            arrowRuleTypeDict.Add("BUILDDAILYBALANCE", "BuildDailyBalance");
            arrowRuleTypeDict.Add("COPYTABLE.COMMONTABLE", "CopyTable.CommonTable");
            arrowRuleTypeDict.Add("CSV2LEDGERRAM.COMMONTABLE", "csv2LedgerRAM.CommonTable");
            arrowRuleTypeDict.Add("GROUPBY", "groupby");
            arrowRuleTypeDict.Add("RULE2LEDGERRAM", "Rule2LedgerRAM");
            arrowRuleTypeDict.Add("REVERSECROSSTAB", "ReverseCrosstab");
            arrowRuleTypeDict.Add("PARALLELPROCESS", "ParallelProcess");
            arrowRuleTypeDict.Add("LEDGERRAMCLONE2SQLSERVER", "LedgerRAMClone2SQLServer");
            arrowRuleTypeDict.Add("LEDGERRAMAPPEND2SQLSERVER", "LedgerRAMAppend2SQLServer");
            arrowRuleTypeDict.Add("DATATABLECLONE2SQLSERVER", "DataTable2SQLServer");
            arrowRuleTypeDict.Add("DATATABLEAPPEND2SQLSERVER", "DataTableAppend2SQLServer");
            arrowRuleTypeDict.Add("FILTERSQLROW.ANDCONDITIONTABLE", "FilterSQLRow.AndConditionTable");
            arrowRuleTypeDict.Add("FILTERSQLROW.ORCONDITIONTABLE", "FilterSQLRow.OrConditionTable");
            arrowRuleTypeDict.Add("REMOVESQLROW.ANDCONDITIONTABLE", "RemoveSQLRow.AndConditionTable");
            arrowRuleTypeDict.Add("REMOVESQLROW.ORCONDITIONTABLE", "RemoveSQLRow.OrConditionTable");
            arrowRuleTypeDict.Add("FILTERSQLROW.ANDDISTINCTTABLE", "FilterSQLRow.AndDistinctTable");
            arrowRuleTypeDict.Add("FILTERSQLROW.ORDISTINCTTABLE", "FilterSQLRow.OrDistinctTable");
            arrowRuleTypeDict.Add("REMOVESQLROW.ANDDISTINCTTABLE", "RemoveSQLRow.AndDistinctTable");
            arrowRuleTypeDict.Add("REMOVESQLROW.ORDISTINCTTABLE", "RemoveSQLRow.OrDistinctTable");
            arrowRuleTypeDict.Add("REMOVESQLTABLE", "RemoveSQLTable");
            arrowRuleTypeDict.Add("REMOVESQLCOLUMN", "RemoveSQLcolumn");
            arrowRuleTypeDict.Add("CREATESQLDATABASE", "CreateSQLDatabase");
            arrowRuleTypeDict.Add("REMOVESQLDATABASE", "REMOVESQLDatabase");
            arrowRuleTypeDict.Add("SQLTABLE2LEDGERRAM", "SQLTable2LedgerRAM");
            arrowRuleTypeDict.Add("DISTINCTSQLTABLE", "DistinctSQLTable");
            arrowRuleTypeDict.Add("GROUPSQLTABLEBY", "GroupSQLTableBy");           


            Dictionary<string, string> nonLedgerRAMDict = new Dictionary<string, string>();
            nonLedgerRAMDict.Add("CSV2LEDGERRAM", "CSV2LEDGERRAM");
            nonLedgerRAMDict.Add("TEXT2LEDGERRAM", "TEXT2LEDGERRAM");
            nonLedgerRAMDict.Add("ONECOLUMN2LEDGERRAM", "ONECOLUMN2LEDGERRAM");
            nonLedgerRAMDict.Add("REVERSECROSSTABCSV2LEDGERRAM", "ReverseCrosstabCSV2LedgerRAM");
            nonLedgerRAMDict.Add("CSV2DATATABLE", "CSV2DataTable");
            nonLedgerRAMDict.Add("CSV2HTML", "CSV2HTML");
            nonLedgerRAMDict.Add("CSV2JSON", "CSV2JSON");
            nonLedgerRAMDict.Add("CSV2XML", "CSV2XML");
            nonLedgerRAMDict.Add("DATATABLE2LEDGERRAM", "DataTable2LEDGERRAM");
            nonLedgerRAMDict.Add("DATATABLE2CSV", "DataTable2CSV");
            nonLedgerRAMDict.Add("DATATABLE2HTML", "DataTable2HTML");
            nonLedgerRAMDict.Add("DATATABLE2JSON", "DataTable2JSON");
            nonLedgerRAMDict.Add("DATATABLE2XML", "DataTable2XML");
           // nonLedgerRAMDict.Add("LEDGERRAMCLONE2SQLSERVER", "LedgerRAMClone2SQLServer");

            // AmendColumnName
            Dictionary<string, Dictionary<int, string>> sourceColumnName = new Dictionary<string, Dictionary<int, string>>();

            // AmendDate                
            Dictionary<string, Dictionary<int, int>> addDay = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> addMonth = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> addYear = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> day = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> month = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> year = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> startMonth = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> startDay = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> startWeek = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> nextPeriodAddDay = new Dictionary<string, Dictionary<int, int>>();            

            // AmendDateFormat                
            Dictionary<string, Dictionary<int, string>> sourceDateFormat = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> resultDateFormat = new Dictionary<string, Dictionary<int, string>>();

            //Amortization                
            Dictionary<string, Dictionary<int, string>> assetID = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> acquisition = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> residual = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> totalTenor = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> startDate = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> endDate = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, List<string>>> amortizationMethod = new Dictionary<string, Dictionary<int, List<string>>>();

            // AppendRow
            Dictionary<string, Dictionary<int, Dictionary<string, string>>> appendRow = new Dictionary<string, Dictionary<int, Dictionary<string, string>>>();

            // Block
            Dictionary<string, Dictionary<int, string>> sourceBlock = new Dictionary<string, Dictionary<int, string>>();

            // BuildBalance                
            Dictionary<string, Dictionary<int, string>> masterTable = new Dictionary<string, Dictionary<int, string>>();

            // BuildBalanceSetting
            string currentBuildBalanceSetting = null;

            // CellName and ColumnName
            Dictionary<string, Dictionary<int, List<string>>> cellName = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> columnName = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> functionName = new Dictionary<string, Dictionary<int, List<string>>>();

            // ComputeColumn
            Dictionary<string, Dictionary<int, string>> resultColumnName = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, string> calcNumberDict = new Dictionary<string, string>();
            calcNumberDict.Add("ADD", "Add");
            calcNumberDict.Add("SUBTRACT", "Subtract");
            calcNumberDict.Add("MULTIPLY", "Multiply");
            calcNumberDict.Add("DIVIDE", "Divide");
            calcNumberDict.Add("SUM", "sum");
            calcNumberDict.Add("MIN", "Min");
            calcNumberDict.Add("MAX", "Max");
            calcNumberDict.Add("AVERAGE", "Average");
            calcNumberDict.Add("COUNT", "Count");
            calcNumberDict.Add("CELLADDRESS", "CellAddress");
            Dictionary<string, string> calcTextDict = new Dictionary<string, string>();
            calcTextDict.Add("COMBINETEXT", "Text");
            Dictionary<string, Dictionary<int, List<int>>> decimalPlace = new Dictionary<string, Dictionary<int, List<int>>>();

            // Condition 
            Dictionary<string, Dictionary<int, string>> conditionType = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, string> conditionTypeDict = new Dictionary<string, string>();
            conditionTypeDict.Add("ANDCONDITION2ACTION", "And");
            conditionTypeDict.Add("ORCONDITION2ACTION", "Or");
            conditionTypeDict.Add("ANDCONDITION2CELL", "And");
            conditionTypeDict.Add("ORCONDITION2CELL", "Or");
            Dictionary<string, Dictionary<int, List<string>>> selectedCellName = new Dictionary<string, Dictionary<int, List<string>>>();

            // Conversion
            Dictionary<string, Dictionary<int, string>> sourceTable = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> resultTable = new Dictionary<string, Dictionary<int, string>>();

            // Crosstab
            Dictionary<string, Dictionary<int, List<string>>> xColumnName = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> yColumnName = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> crosstabAggregateFunction = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> crosstabAggregateByColumnName = new Dictionary<string, Dictionary<int, List<string>>>();

            // Filter 
            Dictionary<string, Dictionary<int, string>> filterType = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>> compareOperator = new Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>>();
            Dictionary<string, Dictionary<int, Dictionary<int, bool>>> isRangeExist = new Dictionary<string, Dictionary<int, Dictionary<int, bool>>>();
            Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>> selectedTextNumber = new Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>>();
            Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>> selectedText = new Dictionary<string, Dictionary<int, Dictionary<int, List<string>>>>();
            Dictionary<string, Dictionary<int, Dictionary<int, List<double>>>> selectedNumber = new Dictionary<string, Dictionary<int, Dictionary<int, List<double>>>>();

            Dictionary<string, string> filterTypeDict = new Dictionary<string, string>();
            filterTypeDict.Add("ANDFILTER", "And");
            filterTypeDict.Add("ORFILTER", "Or");
            filterTypeDict.Add("FILTERSQLROW.ANDCONDITION", "And");
            filterTypeDict.Add("FILTERSQLROW.ORCONDITION", "Or");
            filterTypeDict.Add("REMOVESQLROW.ANDCONDITION", "And");
            filterTypeDict.Add("REMOVESQLROW.ORCONDITION", "Or");

            Dictionary<string, string> compareOperatorDict = new Dictionary<string, string>();
            compareOperatorDict.Add(">=", "Greater Than or Equal");
            compareOperatorDict.Add(">", "Greater Than");
            compareOperatorDict.Add("<=", "Less Than or Equal");
            compareOperatorDict.Add("<", "Less Than");
            compareOperatorDict.Add("!=", "Not Equal");
            compareOperatorDict.Add("=", "Equal");
            compareOperatorDict.Add("..", "Range");
            int filterConditionCount;
            int selectedColumnNameCount;
            int selectedCellNameCount;

            // FileList2LedgerRAM             
            Dictionary<string, Dictionary<int, string>> folderPath = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> fileFilter = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> subDirectory = new Dictionary<string, Dictionary<int, string>>();

            // GroupBy                
            Dictionary<string, Dictionary<int, List<string>>> aggregateFunction = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> aggregateByColumnName = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, string> aggregateFunctionDict = new Dictionary<string, string>();
            aggregateFunctionDict.Add("COUNT", "Count");
            aggregateFunctionDict.Add("SUM", "Sum");
            aggregateFunctionDict.Add("MAX", "Max");
            aggregateFunctionDict.Add("MIN", "Min");
            aggregateFunctionDict.Add("AVERAGE", "Average");

            // JoinTable
            Dictionary<string, Dictionary<int, string>> leftTable = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> rightTable = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, List<string>>> leftTableColumn = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> rightTableColumn = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, string>> joinTableType = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, string> joinTableTypeDict = new Dictionary<string, string>();
            joinTableTypeDict.Add("INNERJOIN", "InnerJoin");
            joinTableTypeDict.Add("FULLJOIN", "FullJoin");
            joinTableTypeDict.Add("JOINTABLE", "JoinTable");
            joinTableTypeDict.Add("CONDITIONALJOIN", "ConditionalJoin");
            joinTableTypeDict.Add("RESIDUALJOIN", "ResidualJoin");
            int joinTableLeftRightSeparator;

            // LedgerRAMAmend2SQLServer
            Dictionary<string, Dictionary<int, List<string>>> amendKey = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> amendMode = new Dictionary<string, Dictionary<int, List<string>>>();

            // MergeCommonTable
            Dictionary<string, Dictionary<int, List<string>>> tableName = new Dictionary<string, Dictionary<int, List<string>>>();

            // OrderBy  
            Dictionary<string, Dictionary<int, Dictionary<string, string>>> orderByColumnName = new Dictionary<string, Dictionary<int, Dictionary<string, string>>>();

            // Period
            Dictionary<string, Dictionary<int, string>> periodType = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> cultureOption = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, string>> periodDateColumn = new Dictionary<string, Dictionary<int, string>>();         
            Dictionary<string, Dictionary<int, int>> periodStartDayNumber = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> periodStartMonthNumber = new Dictionary<string, Dictionary<int, int>>();
            Dictionary<string, Dictionary<int, int>> periodStartWeekNumber = new Dictionary<string, Dictionary<int, int>>();          

            // ReplaceRule
            List<string> replaceRule = new List<string>();

            // SQLStatement

            Dictionary<string, Dictionary<int, string>> sqlStatement = new Dictionary<string, Dictionary<int, string>>();

            // Table2Cell              
            Dictionary<string, Dictionary<int, string>> resultCell = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, string> calc2CellDict = new Dictionary<string, string>();
            calc2CellDict.Add("SUM", "Sum");
            calc2CellDict.Add("COUNT", "Count");
            calc2CellDict.Add("MAX", "Max");
            calc2CellDict.Add("MIN", "Min");
            calc2CellDict.Add("AVERAGE", "Average");
            calc2CellDict.Add("CELLADDRESS", "CellAddress");

            // VoucherEntry
            Dictionary<string, Dictionary<int, List<string>>> amount = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> drCr = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> account = new Dictionary<string, Dictionary<int, List<string>>>();
            Dictionary<string, Dictionary<int, List<string>>> excludeBalanceGroupBy = new Dictionary<string, Dictionary<int, List<string>>>();

            Dictionary<string, string> debitDict = new Dictionary<string, string>();
            debitDict.Add("D", "Debit");
            debitDict.Add("DR", "Debit");
            debitDict.Add("DEBIT", "Debit");

            Dictionary<string, string> creditDict = new Dictionary<string, string>();
            creditDict.Add("C", "Debit");
            creditDict.Add("CR", "Debit");
            creditDict.Add("CREDIT", "Debit");

            Dictionary<string, string> balance = new Dictionary<string, string>();
            balance.Add("B", "Balance");
            balance.Add("BAL", "Balance");
            balance.Add("BALANCE", "Balance");
            balance.Add("BALANCE ENTRY", "Balance");            

            Dictionary<string, Dictionary<int, string>> ruleType = new Dictionary<string, Dictionary<int, string>>();
            Dictionary<string, Dictionary<int, StringBuilder>> recognizedRule = new Dictionary<string, Dictionary<int, StringBuilder>>();
            Dictionary<string, Dictionary<int, bool>> isParallelProcess = new Dictionary<string, Dictionary<int, bool>>();
            Dictionary<string, Dictionary<int, string>> tableSizeMessage = new Dictionary<string, Dictionary<int, string>>();

            string appendRowColumnName; string appendRowColumnValue; 
            bool arrow; int assignment;
            string bracket; string bracketName;
            int convertTo; string curlyBracket;
            int fullStop;
            bool isProcessExist;
            string squareBracket; int startAddress;
            int verticalBar;

            if (isExecute == true)
            {
                foreach (var block in ruleDetail)
                {
                    currentBlock = block.Key;
                    
                    account.Add(currentBlock, new Dictionary<int, List<string>>());
                    acquisition.Add(currentBlock, new Dictionary<int, string>());
                    addDay.Add(currentBlock, new Dictionary<int, int>());
                    addMonth.Add(currentBlock, new Dictionary<int, int>());
                    addYear.Add(currentBlock, new Dictionary<int, int>());
                    afterRuleTypeParameter.Add(currentBlock, new Dictionary<int, List<string>>());
                    aggregateByColumnName.Add(currentBlock, new Dictionary<int, List<string>>());
                    aggregateFunction.Add(currentBlock, new Dictionary<int, List<string>>());
                    amendKey.Add(currentBlock, new Dictionary<int, List<string>>());
                    amendMode.Add(currentBlock, new Dictionary<int, List<string>>());
                    amortizationMethod.Add(currentBlock, new Dictionary<int, List<string>>());
                    amount.Add(currentBlock, new Dictionary<int, List<string>>());
                    appendRow.Add(currentBlock, new Dictionary<int, Dictionary<string, string>>());
                    assetID.Add(currentBlock, new Dictionary<int, string>());                  
                    cellName.Add(currentBlock, new Dictionary<int, List<string>>());
                    columnName.Add(currentBlock, new Dictionary<int, List<string>>());             
                    compareOperator.Add(currentBlock, new Dictionary<int, Dictionary<int, List<string>>>());                  
                    conditionType.Add(currentBlock, new Dictionary<int, string>());
                    crosstabAggregateByColumnName.Add(currentBlock, new Dictionary<int, List<string>>());
                    crosstabAggregateFunction.Add(currentBlock, new Dictionary<int, List<string>>());
                    cultureOption.Add(currentBlock, new Dictionary<int, string>());
                    day.Add(currentBlock, new Dictionary<int, int>());
                    decimalPlace.Add(currentBlock, new Dictionary<int, List<int>>());
                    endDate.Add(currentBlock, new Dictionary<int, string>());
                    excludeBalanceGroupBy.Add(currentBlock, new Dictionary<int, List<string>>());
                    fileFilter.Add(currentBlock, new Dictionary<int, string>());                   
                    filterType.Add(currentBlock, new Dictionary<int, string>());
                    folderPath.Add(currentBlock, new Dictionary<int, string>());
                    functionName.Add(currentBlock, new Dictionary<int, List<string>>());
                    isParallelProcess.Add(currentBlock, new Dictionary<int, bool>());
                    isRangeExist.Add(currentBlock, new Dictionary<int, Dictionary<int, bool>>());
                    joinTableType.Add(currentBlock, new Dictionary<int, string>());                 
                    masterTable.Add(currentBlock, new Dictionary<int, string>());
                    leftTable.Add(currentBlock, new Dictionary<int, string>());
                    leftTableColumn.Add(currentBlock, new Dictionary<int, List<string>>());
                    month.Add(currentBlock, new Dictionary<int, int>());
                    nextPeriodAddDay.Add(currentBlock, new Dictionary<int, int>());
                    orderByColumnName.Add(currentBlock, new Dictionary<int, Dictionary<string, string>>());
                    periodDateColumn.Add(currentBlock, new Dictionary<int, string>());                   
                    periodStartDayNumber.Add(currentBlock, new Dictionary<int, int>());
                    periodStartMonthNumber.Add(currentBlock, new Dictionary<int, int>());
                    periodStartWeekNumber.Add(currentBlock, new Dictionary<int, int>());
                    periodType.Add(currentBlock, new Dictionary<int, string>());
                    recognizedRule.Add(currentBlock, new Dictionary<int, StringBuilder>());
                    residual.Add(currentBlock, new Dictionary<int, string>());
                    resultCell.Add(currentBlock, new Dictionary<int, string>());
                    resultColumnName.Add(currentBlock, new Dictionary<int, string>());
                    resultDateFormat.Add(currentBlock, new Dictionary<int, string>());
                    resultTable.Add(currentBlock, new Dictionary<int, string>());                  
                    ruleTypeParameter.Add(currentBlock, new Dictionary<int, List<string>>());
                    rightTable.Add(currentBlock, new Dictionary<int, string>());
                    rightTableColumn.Add(currentBlock, new Dictionary<int, List<string>>());
                    ruleType.Add(currentBlock, new Dictionary<int, string>());                    
                    selectedCellName.Add(currentBlock, new Dictionary<int, List<string>>());
                    selectedNumber.Add(currentBlock, new Dictionary<int, Dictionary<int, List<double>>>());
                    selectedText.Add(currentBlock, new Dictionary<int, Dictionary<int, List<string>>>());
                    selectedTextNumber.Add(currentBlock, new Dictionary<int, Dictionary<int, List<string>>>());
                    sourceBlock.Add(currentBlock, new Dictionary<int, string>());
                    sourceColumnName.Add(currentBlock, new Dictionary<int, string>());
                    sourceDateFormat.Add(currentBlock, new Dictionary<int, string>());
                    sourceTable.Add(currentBlock, new Dictionary<int, string>());
                    sqlStatement.Add(currentBlock, new Dictionary<int, string>());
                    subDirectory.Add(currentBlock, new Dictionary<int, string>());
                    startDate.Add(currentBlock, new Dictionary<int, string>());
                    startMonth.Add(currentBlock, new Dictionary<int, int>());
                    startDay.Add(currentBlock, new Dictionary<int, int>());
                    startWeek.Add(currentBlock, new Dictionary<int, int>());                    
                    tableName.Add(currentBlock, new Dictionary<int, List<string>>());
                    tableSizeMessage.Add(currentBlock, new Dictionary<int, string>());
                    totalTenor.Add(currentBlock, new Dictionary<int, string>());
                    drCr.Add(currentBlock, new Dictionary<int, List<string>>());
                    xColumnName.Add(currentBlock, new Dictionary<int, List<string>>());
                    yColumnName.Add(currentBlock, new Dictionary<int, List<string>>());
                    year.Add(currentBlock, new Dictionary<int, int>());

                    for (int r = 0; r < ruleDetail[currentBlock].Count; r++)
                    {
                        isParallelProcess[currentBlock][r] = false;

                        appendRowColumnName = null; appendRowColumnValue = null;
                        at = 0; arrow = false; assignment = 0;
                        bracket = ""; bracketName = null;
                        convertTo = 0; curlyBracket = "";
                        filterConditionCount = 0;
                        fullStop = 0;
                        isProcessExist = false;
                        joinTableLeftRightSeparator = 0;
                        selectedColumnNameCount = 0; selectedCellNameCount = 0; squareBracket = null; startAddress = 0;
                        verticalBar = 0;

                        columnName[currentBlock].Add(r, new List<string>());
                        cellName[currentBlock].Add(r, new List<string>());
                        functionName[currentBlock].Add(r, new List<string>());
                        recognizedRule[currentBlock].Add(r, new StringBuilder());

                        // Record Rule Type 
                        u = 0;
                        for (int i = 0; i < ruleDetail[currentBlock][r].Length; i++)
                        {
                            if (i < ruleDetail[currentBlock][r].Length - 1)
                            {
                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "/" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == "/")
                                    i++;
                            }

                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "{")
                            {
                                curlyBracket = "open";

                                if (ruleDetail[currentBlock][r].ToString().Substring(startAddress, i).Trim().ToUpper() != "REPLACERULE")
                                    ruleType[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress, i).Trim());

                                if (ruleDetail[currentBlock][r].ToString().Substring(startAddress, i).Trim().ToUpper() == "REPLACERULE")
                                {
                                    ruleType[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress, i).Trim());

                                    startAddress = i;

                                    do
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "," || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            replaceRule.Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                curlyBracket = "close";

                                            startAddress = i;
                                        }

                                        i++;


                                    } while (curlyBracket == "open");

                                }
                                startAddress = i;

                                u = i;
                            }
                        }

                        for (int i = 0; i < replaceRule.Count; i++)
                        {
                            if (newRule.Count != 0)
                            {
                                var currentRule = ruleDetail[currentBlock][r].ToString().Replace(replaceRule[i], newRule[i]);
                                ruleDetail[currentBlock][r].Clear();
                                ruleDetail[currentBlock][r].Append(currentRule);
                            }
                        }

                        // recognize source and result for Conversion 
                        if (ruleType[currentBlock].ContainsKey(r))
                        {
                            var currentCommand = ruleType[currentBlock][r].ToUpper().Trim();

                            if (arrowRuleTypeDict.ContainsKey(currentCommand))
                            {
                                if (!afterRuleTypeParameter[currentBlock].ContainsKey(r))
                                    afterRuleTypeParameter[currentBlock].Add(r, new List<string>());

                                if (!aggregateFunction[currentBlock].ContainsKey(r))
                                    aggregateFunction[currentBlock].Add(r, new List<string>());

                                if (!aggregateByColumnName[currentBlock].ContainsKey(r))
                                    aggregateByColumnName[currentBlock].Add(r, new List<string>());

                                if (!decimalPlace[currentBlock].ContainsKey(r))
                                    decimalPlace[currentBlock].Add(r, new List<int>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            if (squareBracket != "close")
                                                columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                            arrow = true;
                                            startAddress = i + 1;
                                        }

                                        if (arrow == false)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "@")
                                            {
                                                at++;

                                                if (i - startAddress - 1 > 1)
                                                {
                                                    columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                            {
                                                verticalBar++;
                                                sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            else if (at == 0 && (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "," || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "[" || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "]"))
                                            {
                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "[")
                                                    squareBracket = "open";

                                                else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "]")
                                                    squareBracket = "close";

                                                columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            /*
                                            else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "@")
                                            {
                                               // at++;
                                               // columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }
                                            */

                                            else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {
                                                convertTo = 1;

                                                if (at == 0)
                                                {

                                                    if (nonLedgerRAMDict.ContainsKey(currentCommand) && !sourceTable[currentBlock].ContainsKey(r) && columnName[currentBlock][r].Count == 0)
                                                        sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                    else
                                                        columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }
                                                else
                                                {
                                                    masterTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                startAddress = i;
                                            }

                                            else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            {
                                                if (at == 0 && convertTo == 0)
                                                    columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                else if (at == 1 && convertTo == 0)
                                                {
                                                    masterTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                else if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }
                                        }
                                        else
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {
                                                convertTo = 1;
                                                startAddress = i;
                                            }

                                            else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                            {
                                                bracket = "open";
                                                bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                startAddress = i;
                                            }

                                            else if (fullStop == 0 && (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")" || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "."))
                                            {
                                                bracket = "close";

                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ".")
                                                    fullStop++;

                                                var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                                if (calcNumberDict.ContainsKey(bracketName.ToUpper()) || calcTextDict.ContainsKey(bracketName.ToUpper()))
                                                {
                                                    afterRuleTypeParameter[currentBlock][r].Add(bracketName);
                                                    aggregateFunction[currentBlock][r].Add(bracketName);

                                                    if (currentCommand == "GROUPBY" && bracketName.ToUpper() == "COUNT")
                                                        aggregateByColumnName[currentBlock][r].Add("Null");
                                                    else
                                                        aggregateByColumnName[currentBlock][r].Add(value);

                                                    if (fullStop == 0)
                                                        decimalPlace[currentBlock][r].Add(999);
                                                }

                                                startAddress = i;
                                            }

                                            else if (fullStop == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bool success = int.TryParse(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim(), out int number);

                                                if (success == true)
                                                    decimalPlace[currentBlock][r].Add(number);
                                                else
                                                    decimalPlace[currentBlock][r].Add(999);

                                                startAddress = i;
                                            }

                                            else if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (convertTo == 1)
                                                    if (!resultTable[currentBlock].ContainsKey(r))
                                                        resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                        }
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r]);

                                    if (verticalBar == 1)
                                        recognizedRule[currentBlock][r].Append(" | ");

                                    if (squareBracket == "close")
                                        recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][0] + "[" + columnName[currentBlock][r][1] + "]");
                                    else
                                    {
                                        for (int j = 0; j < columnName[currentBlock][r].Count; j++)
                                        {
                                            if (j < columnName[currentBlock][r].Count - 1)
                                                recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j] + ", ");
                                            else
                                                recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j]);
                                        }
                                    }

                                    if (afterRuleTypeParameter[currentBlock][r].Count > 0)
                                        recognizedRule[currentBlock][r].Append(" => ");

                                    for (int i = 0; i < afterRuleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (calcNumberDict.ContainsKey(afterRuleTypeParameter[currentBlock][r][i].ToUpper()) || calcTextDict.ContainsKey(afterRuleTypeParameter[currentBlock][r][i].ToUpper()))
                                            recognizedRule[currentBlock][r].Append(aggregateFunction[currentBlock][r][i]);

                                        if (currentCommand == "GROUPBY" && aggregateByColumnName[currentBlock][r][i].ToUpper() == "NULL")
                                            recognizedRule[currentBlock][r].Append("(");
                                        else
                                            recognizedRule[currentBlock][r].Append("(" + aggregateByColumnName[currentBlock][r][i]);

                                        if (decimalPlace[currentBlock][r][i] != 999)
                                            recognizedRule[currentBlock][r].Append("." + decimalPlace[currentBlock][r][i]);

                                        recognizedRule[currentBlock][r].Append(")");

                                        if (i < afterRuleTypeParameter[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(" ");
                                    }

                                    if (resultTable[currentBlock].ContainsKey(r))
                                    {
                                        if (masterTable[currentBlock].ContainsKey(r))
                                            recognizedRule[currentBlock][r].Append(" @ " + masterTable[currentBlock][r]);

                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    }
                                    else
                                    {
                                        if (masterTable[currentBlock].ContainsKey(r))
                                            recognizedRule[currentBlock][r].Append(" @ " + masterTable[currentBlock][r]);

                                        recognizedRule[currentBlock][r].Append("}");
                                    }
                                }
                            }

                            else if (currentCommand == "AMENDCOLUMNNAME")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=")
                                        {
                                            assignment = 1;
                                            sourceColumnName[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }
                                        if (assignment == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            resultColumnName[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }
                                        if (assignment == 1 && convertTo == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";
                                            resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }
                                        if (assignment == 1 && convertTo == 0 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";
                                            resultColumnName[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }
                                    }

                                    if (convertTo == 1)
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            if (!resultTable[currentBlock].ContainsKey(r))
                                                resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    recognizedRule[currentBlock][r].Append(sourceColumnName[currentBlock][r] + " = " + resultColumnName[currentBlock][r]);

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "AMENDDATE" || currentCommand == "REVERSEMONTHLYVOUCHER" || currentCommand == "REVERSEDAILYVOUCHER" || currentCommand == "REVERSEWEEKLYVOUCHER")
                            {
                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "@")
                                        {
                                            at++;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            arrow = true;
                                            columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i + 1;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                        {
                                            columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (arrow == true)
                                        {

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                            {
                                                bracket = "open";
                                                bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bracket = "close";
                                                bool success = int.TryParse(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim(), out int number);

                                                if (success == true)
                                                {
                                                    if (bracketName.ToUpper() == "ADDDAY")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("AddDay");
                                                        addDay[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "ADDMONTH")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("AddMonth");
                                                        addMonth[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "ADDYEAR")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("AddYear");
                                                        addYear[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "DAY")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("Day");
                                                        day[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "MONTH")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("Month");
                                                        month[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "YEAR")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("Year");
                                                        year[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "STARTMONTH")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("StartMonth");
                                                        startMonth[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "STARTDAY")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("StartDay");
                                                        startDay[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "STARTWEEK")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("StartWeek");
                                                        startWeek[currentBlock].Add(r, number);
                                                    }

                                                    else if (bracketName.ToUpper() == "NEXTPERIODADDDAY")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add("NextPeriodAddDay");
                                                        nextPeriodAddDay[currentBlock].Add(r, number);
                                                    }
                                                }
                                                else

                                                {
                                                    if (bracketName.ToUpper() == "PERIODTYPE")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                        periodType[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                    }

                                                    if (bracketName.ToUpper() == "CULTUREOPTION")
                                                    {
                                                        ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                        cultureOption[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                    }
                                                }

                                                startAddress = i;
                                            }
                                        }

                                        if (at == 1)
                                        {
                                            if (convertTo == 0)
                                            {
                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~" || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                {
                                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                                        convertTo = 1;

                                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                        curlyBracket = "close";

                                                    masterTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                    startAddress = i;
                                                }
                                            }
                                            else
                                            {
                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                {
                                                    curlyBracket = "close";

                                                    if (convertTo == 1)
                                                        resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                    else
                                                        columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                    startAddress = i;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {
                                                convertTo = 1;
                                                startAddress = i;
                                            }

                                            if (convertTo == 0 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                curlyBracket = "close";

                                            if (convertTo == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            {
                                                curlyBracket = "close";

                                                if (!resultCell[currentBlock].ContainsKey(r))
                                                    resultCell[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }
                                        }
                                    }
                                }


                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int j = 0; j < columnName[currentBlock][r].Count; j++)
                                    {
                                        if (j < columnName[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j] + ", ");
                                        else
                                            if (resultTable[currentBlock].ContainsKey(r))
                                            recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j]);
                                    }

                                    recognizedRule[currentBlock][r].Append(" => ");

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ADDDAY")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + addDay[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ADDMONTH")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + addMonth[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ADDYEAR")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + addYear[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "DAY")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + day[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "MONTH")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + month[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "YEAR")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + year[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTMONTH")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + startMonth[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTDAY")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + startDay[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTWEEK")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + startWeek[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "NEXTPERIODADDDAY")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + nextPeriodAddDay[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "PERIODTYPE")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + periodType[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "CULTUREOPTION")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + cultureOption[currentBlock][r] + ")");
                                    }


                                    if (masterTable[currentBlock].ContainsKey(r))
                                    {
                                        if (resultTable[currentBlock].ContainsKey(r))
                                        {
                                            if (masterTable[currentBlock].ContainsKey(r))
                                                recognizedRule[currentBlock][r].Append(" @ " + masterTable[currentBlock][r] + " ~ " + resultTable[currentBlock][r] + "}");
                                        }
                                        else
                                        {
                                            if (masterTable[currentBlock].ContainsKey(r))
                                                recognizedRule[currentBlock][r].Append(" @ " + masterTable[currentBlock][r] + "}");
                                        }
                                    }
                                    else
                                    {
                                        if (resultCell[currentBlock].ContainsKey(r))
                                            recognizedRule[currentBlock][r].Append(" ~ " + resultCell[currentBlock][r] + "}");
                                        else
                                            recognizedRule[currentBlock][r].Append("}");
                                    }
                                }
                            }

                            else if (currentCommand == "AMENDDATEFORMAT")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) != ">")
                                        {
                                            sourceDateFormat[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            arrow = true;
                                            columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i + 1;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                        {
                                            columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (arrow == true)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {
                                                convertTo = 1;
                                                resultDateFormat[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            if (convertTo == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            {
                                                curlyBracket = "close";

                                                if (!resultCell[currentBlock].ContainsKey(r))
                                                    resultCell[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }

                                            if (convertTo == 0 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            {
                                                curlyBracket = "close";
                                                resultDateFormat[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }
                                        }
                                    }
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int j = 0; j < columnName[currentBlock][r].Count; j++)
                                    {
                                        if (j < columnName[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j] + ", ");
                                        else
                                            recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j]);
                                    }

                                    recognizedRule[currentBlock][r].Append(" => ");

                                    recognizedRule[currentBlock][r].Append(sourceDateFormat[currentBlock][r] + " = " + resultDateFormat[currentBlock][r]);

                                    if (resultCell[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultCell[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "AMORTIZATION")
                            {
                                if (!amortizationMethod[currentBlock].ContainsKey(r))
                                    amortizationMethod[currentBlock].Add(r, new List<string>());

                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if (bracketName.ToUpper() == "ASSETID")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("AssetID");
                                                assetID[currentBlock].Add(r, value);
                                            }

                                            if (bracketName.ToUpper() == "ACQUISITION")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("Acquisition");
                                                acquisition[currentBlock].Add(r, value);
                                            }

                                            if (bracketName.ToUpper() == "RESIDUAL")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("Residual");
                                                residual[currentBlock].Add(r, value);
                                            }

                                            else if (bracketName.ToUpper() == "TOTALTENOR")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("TotalTenor");
                                                totalTenor[currentBlock].Add(r, value);
                                            }

                                            else if (bracketName.ToUpper() == "STARTDATE")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("StartDate");
                                                startDate[currentBlock].Add(r, value);
                                            }

                                            else if (bracketName.ToUpper() == "ENDDATE")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("EndDate");
                                                endDate[currentBlock].Add(r, value);
                                            }

                                            else if (bracketName.ToUpper() == "METHOD")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("Method");
                                                amortizationMethod[currentBlock][r].Add(value);
                                            }

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                        {
                                            amortizationMethod[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ASSETID")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + assetID[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ACQUISITION")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + acquisition[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "RESIDUAL")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + residual[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "TOTALTENOR")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + totalTenor[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTDATE")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + startDate[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "ENDDATE")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + endDate[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "METHOD")
                                        {
                                            for (int j = 0; j < amortizationMethod[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + amortizationMethod[currentBlock][r][j]);

                                                else if (j == amortizationMethod[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + amortizationMethod[currentBlock][r][j] + ")");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + amortizationMethod[currentBlock][r][j]);
                                            }
                                        }
                                    }

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "APPENDROW")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            appendRowColumnName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            startAddress = i;
                                        }

                                        if (bracket == "open" && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";

                                            appendRowColumnValue = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if (!appendRow[currentBlock].ContainsKey(r))
                                                appendRow[currentBlock].Add(r, new Dictionary<string, string>());

                                            appendRow[currentBlock][r].Add(appendRowColumnName, appendRowColumnValue);

                                            startAddress = i;
                                        }
                                    }

                                    if (convertTo == 1)
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            if (!resultTable[currentBlock].ContainsKey(r))
                                                resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    foreach (var pair in appendRow[currentBlock][r])
                                        recognizedRule[currentBlock][r].Append(pair.Key + "(" + pair.Value + ")");

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            // Condition
                            else if (conditionTypeDict.ContainsKey(currentCommand))
                            {
                                if (!conditionType[currentBlock].ContainsKey(r))
                                    conditionType[currentBlock].Add(r, conditionTypeDict[ruleType[currentBlock][r].ToUpper().Trim()]);

                                if (!selectedCellName[currentBlock].ContainsKey(r))
                                    selectedCellName[currentBlock].Add(r, new List<string>());

                                if (!compareOperator[currentBlock].ContainsKey(r))
                                    compareOperator[currentBlock].Add(r, new Dictionary<int, List<string>>());

                                if (!isRangeExist[currentBlock].ContainsKey(r))
                                    isRangeExist[currentBlock].Add(r, new Dictionary<int, bool>());

                                if (!selectedTextNumber[currentBlock].ContainsKey(r))
                                    selectedTextNumber[currentBlock].Add(r, new Dictionary<int, List<string>>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(" && arrow == false)
                                        {
                                            bracket = "open";
                                            selectedCellNameCount++;
                                            selectedCellName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                            filterConditionCount++;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            arrow = true;
                                            startAddress = i + 1;
                                        }

                                        if (bracket == "open")
                                        {
                                            if (compareOperatorDict.ContainsKey(ruleDetail[currentBlock][r].ToString().Substring(i, 2)))
                                            {
                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 2) == "..")
                                                {
                                                    if (!compareOperator[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                        compareOperator[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                    compareOperator[currentBlock][r][selectedCellNameCount - 1].Add(">=");
                                                    compareOperator[currentBlock][r][selectedCellNameCount - 1].Add("<=");

                                                    if (!isRangeExist[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                        isRangeExist[currentBlock][r].Add(selectedCellNameCount - 1, true);

                                                    if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                        selectedTextNumber[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                    selectedTextNumber[currentBlock][r][selectedCellNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }
                                                else
                                                {
                                                    if (!compareOperator[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                        compareOperator[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                    compareOperator[currentBlock][r][selectedCellNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, 2).Trim());
                                                }

                                                i++;
                                                startAddress = i;
                                            }
                                            else if (compareOperatorDict.ContainsKey(ruleDetail[currentBlock][r].ToString().Substring(i, 1)))
                                            {
                                                if (!compareOperator[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                    compareOperator[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                compareOperator[currentBlock][r][selectedCellNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, 1).Trim());

                                                if (!isRangeExist[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                    isRangeExist[currentBlock][r].Add(selectedCellNameCount - 1, false);

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                            {
                                                if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                    selectedTextNumber[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                selectedTextNumber[currentBlock][r][selectedCellNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bracket = "close";

                                                if (isProcessExist == true)
                                                {
                                                    var currentCell = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                    sourceBlock[currentBlock].Add(r, currentCell);

                                                    if (!resultCell[currentBlock].ContainsKey(r))
                                                        resultCell[currentBlock].Add(r, currentCell);
                                                }
                                                else
                                                {
                                                    if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedCellNameCount - 1))
                                                        selectedTextNumber[currentBlock][r].Add(selectedCellNameCount - 1, new List<string>());

                                                    selectedTextNumber[currentBlock][r][selectedCellNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                startAddress = i;
                                            }
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultCell[currentBlock].ContainsKey(r))
                                                    resultCell[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                        if (arrow == true)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                            {
                                                bracket = "open";
                                                var currentCell = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                                if (currentCell.ToUpper() == "PROCESS")
                                                    isProcessExist = true;

                                                startAddress = i;
                                            }
                                        }
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    for (int j = 0; j < selectedCellName[currentBlock][r].Count; j++)
                                    {
                                        if (selectedCellName[currentBlock][r].Count == selectedTextNumber[currentBlock][r].Count && selectedCellName[currentBlock][r].Count == compareOperator[currentBlock][r].Count)
                                        {
                                            recognizedRule[currentBlock][r].Append(selectedCellName[currentBlock][r][j] + "(");

                                            if (isRangeExist[currentBlock][r].ContainsKey(j))
                                            {
                                                if (isRangeExist[currentBlock][r][j] == true)
                                                    recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][0] + ".." + selectedTextNumber[currentBlock][r][j][1] + ")");

                                                if (isRangeExist[currentBlock][r][j] == false)
                                                {
                                                    for (int k = 0; k < compareOperator[currentBlock][r][j].Count; k++)
                                                    {
                                                        if (compareOperator[currentBlock][r][j].Count == selectedTextNumber[currentBlock][r][j].Count)
                                                        {
                                                            recognizedRule[currentBlock][r].Append(compareOperator[currentBlock][r][j][k]);

                                                            if (k < compareOperator[currentBlock][r][j].Count - 1)
                                                                recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ", ");
                                                            else
                                                                recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ")");
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                for (int k = 0; k < compareOperator[currentBlock][r][j].Count; k++)
                                                {
                                                    if (compareOperator[currentBlock][r][j].Count == selectedTextNumber[currentBlock][r][j].Count)
                                                    {
                                                        recognizedRule[currentBlock][r].Append(compareOperator[currentBlock][r][j][k]);

                                                        if (k < compareOperator[currentBlock][r][j].Count - 1)
                                                            recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ", ");
                                                        else
                                                            recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ")");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (resultCell[currentBlock].ContainsKey(r))
                                    {
                                        if (ruleType[currentBlock][r].ToUpper().Trim().Contains("ACTION"))
                                            recognizedRule[currentBlock][r].Append(" => Process(" + resultCell[currentBlock][r] + ")}");

                                        if (ruleType[currentBlock][r].ToUpper().Trim().Contains("CELL"))
                                            recognizedRule[currentBlock][r].Append(" ~ " + resultCell[currentBlock][r] + "}");
                                    }
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "CROSSTAB" || currentCommand == "CROSSTABSQLTABLE")
                            {
                                if (!xColumnName[currentBlock].ContainsKey(r))
                                    xColumnName[currentBlock].Add(r, new List<string>());

                                if (!yColumnName[currentBlock].ContainsKey(r))
                                    yColumnName[currentBlock].Add(r, new List<string>());

                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                if (!afterRuleTypeParameter[currentBlock].ContainsKey(r))
                                    afterRuleTypeParameter[currentBlock].Add(r, new List<string>());

                                if (!crosstabAggregateFunction[currentBlock].ContainsKey(r))
                                    crosstabAggregateFunction[currentBlock].Add(r, new List<string>());

                                if (!crosstabAggregateByColumnName[currentBlock].ContainsKey(r))
                                    crosstabAggregateByColumnName[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (arrow == false)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                            {
                                                sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                            {
                                                bracket = "open";
                                                bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bracket = "close";
                                                var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                                if (bracketName.ToUpper() == "X")
                                                {
                                                    ruleTypeParameter[currentBlock][r].Add("X");
                                                    xColumnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                if (bracketName.ToUpper() == "Y")
                                                {
                                                    ruleTypeParameter[currentBlock][r].Add("Y"); 
                                                    yColumnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                            {
                                                if (bracketName.ToUpper() == "X")
                                                {
                                                    xColumnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                    startAddress = i;
                                                }

                                                if (bracketName.ToUpper() == "Y")
                                                {
                                                    yColumnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                    startAddress = i;
                                                }
                                            }
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            arrow = true;
                                            startAddress = i + 1;                                           
                                        }

                                        if (arrow == true)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {
                                                convertTo = 1;
                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                            {                                              
                                                bracket = "open";
                                                bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bracket = "close";
                                                var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                                if (bracketName.ToUpper() == "SUM" || bracketName.ToUpper() == "MAX" || bracketName.ToUpper() == "MIN")
                                                {
                                                    afterRuleTypeParameter[currentBlock][r].Add(bracketName);
                                                    crosstabAggregateFunction[currentBlock][r].Add(bracketName);
                                                    crosstabAggregateByColumnName[currentBlock][r].Add(value);
                                                }                                              

                                                if (bracketName.ToUpper() == "COUNT")
                                                {
                                                    afterRuleTypeParameter[currentBlock][r].Add("Count");
                                                    crosstabAggregateFunction[currentBlock][r].Add("Count");
                                                    crosstabAggregateByColumnName[currentBlock][r].Add("Null");
                                                }

                                                startAddress = i;
                                            }
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "X")
                                        {
                                            for (int j = 0; j < xColumnName[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + xColumnName[currentBlock][r][j]);

                                                else if (j == xColumnName[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + xColumnName[currentBlock][r][j] + ") ");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + xColumnName[currentBlock][r][j]);
                                            }

                                            if(xColumnName[currentBlock][r].Count == 1)
                                                recognizedRule[currentBlock][r].Append(") ");
                                        }

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "Y")
                                        {
                                            for (int j = 0; j < yColumnName[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + yColumnName[currentBlock][r][j]);

                                                else if (j == yColumnName[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + yColumnName[currentBlock][r][j] + ")");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + yColumnName[currentBlock][r][j]);
                                            }

                                            if (yColumnName[currentBlock][r].Count == 1)
                                                recognizedRule[currentBlock][r].Append(") ");
                                        }
                                    }

                                    recognizedRule[currentBlock][r].Append(" => ");

                                    for (int i = 0; i < afterRuleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (afterRuleTypeParameter[currentBlock][r][i].ToUpper() == "SUM" || afterRuleTypeParameter[currentBlock][r][i].ToUpper() == "MAX" || afterRuleTypeParameter[currentBlock][r][i].ToUpper() == "MIN")
                                            recognizedRule[currentBlock][r].Append(crosstabAggregateFunction[currentBlock][r][i] + "(" + crosstabAggregateByColumnName[currentBlock][r][i] + ") ");

                                        if (afterRuleTypeParameter[currentBlock][r][i].ToUpper() == "COUNT")
                                            recognizedRule[currentBlock][r].Append(crosstabAggregateFunction[currentBlock][r][i] + "() ");
                                    }                                  

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append("~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "DATE2MONTHLYPERIOD" || currentCommand == "DATE2DAILYPERIOD" || currentCommand == "DATE2WEEKLYPERIOD")
                            {
                                if (!orderByColumnName[currentBlock].ContainsKey(r))
                                    orderByColumnName[currentBlock].Add(r, new Dictionary<string, string>());

                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if (bracketName.ToUpper() == "PERIODTYPE")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                periodType[currentBlock].Add(r, value);
                                            }

                                            if (bracketName.ToUpper() == "CULTUREOPTION")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                cultureOption[currentBlock].Add(r, value);
                                            }

                                            if (bracketName.ToUpper() == "DATECOLUMN")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                periodDateColumn[currentBlock].Add(r, value);
                                            }

                                            if (bracketName.ToUpper() == "STARTDAY")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);

                                                bool success = int.TryParse(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim(), out int _month);

                                                if(success == true)                                                
                                                    periodStartDayNumber[currentBlock].Add(r, _month);                                                                                                 
                                            }

                                            if (bracketName.ToUpper() == "STARTMONTH")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);

                                                bool success = int.TryParse(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim(), out int _month);

                                                if (success == true)                                               
                                                    periodStartMonthNumber[currentBlock].Add(r, _month);                                                                                                
                                            }

                                            if (bracketName.ToUpper() == "STARTWEEK")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);

                                                bool success = int.TryParse(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim(), out int _month);

                                                if (success == true)                                               
                                                    periodStartWeekNumber[currentBlock].Add(r, _month);                                                                                                 
                                            }

                                            startAddress = i;
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }                             

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");                              

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "PERIODTYPE")
                                            recognizedRule[currentBlock][r].Append("PeriodType(" + periodType[currentBlock][r] + ")");                                      

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "CULTUREOPTION")
                                            recognizedRule[currentBlock][r].Append("CultureOption(" + cultureOption[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "DATECOLUMN")
                                            recognizedRule[currentBlock][r].Append("DateColumn(" + periodDateColumn[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTDAY")
                                            recognizedRule[currentBlock][r].Append("StartDay(" + periodStartDayNumber[currentBlock][r].ToString() + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTMONTH")
                                            recognizedRule[currentBlock][r].Append("StartMonth(" + periodStartMonthNumber[currentBlock][r].ToString() + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "STARTWEEK")
                                            recognizedRule[currentBlock][r].Append("StartWeek(" + periodStartWeekNumber[currentBlock][r].ToString() + ")");

                                        if (i == 0)
                                            recognizedRule[currentBlock][r].Append(" ");
                                    }

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append("~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }
                          
                            else if (currentCommand == "FILELIST2LEDGERRAM" || currentCommand == "MANYCSV2LEDGERRAM" || currentCommand == "REVERSEMANYCROSSTABCSV2LEDGERRAM")
                            {
                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            var bracketValue = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            
                                            if (bracketName.ToUpper() == "FOLDERPATH")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("FolderPath");
                                                folderPath[currentBlock].Add(r, bracketValue);
                                            }

                                            else if (bracketName.ToUpper() == "FILEFILTER")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("FileFilter");
                                                fileFilter[currentBlock].Add(r, bracketValue);
                                            }

                                            else if (bracketName.ToUpper() == "SUBDIRECTORY")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("Subdirectory");
                                                subDirectory[currentBlock].Add(r, bracketValue);
                                            }                                              
                                            
                                            startAddress = i;
                                        }

                                        if (convertTo == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";                                          

                                            if (!resultTable[currentBlock].ContainsKey(r))
                                                resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                        }
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");                                    

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "FOLDERPATH")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + folderPath[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "FILEFILTER")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + fileFilter[currentBlock][r] + ")");

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "SUBDIRECTORY")
                                            recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + subDirectory[currentBlock][r] + ")");                                      
                                    }
                                   
                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            // Filter
                            else if (filterTypeDict.ContainsKey(currentCommand))
                            {
                                if (!filterType[currentBlock].ContainsKey(r))
                                    filterType[currentBlock].Add(r, filterTypeDict[ruleType[currentBlock][r].ToUpper().Trim()]);                                  

                                if (!compareOperator[currentBlock].ContainsKey(r))
                                    compareOperator[currentBlock].Add(r, new Dictionary<int, List<string>>());

                                if (!isRangeExist[currentBlock].ContainsKey(r))
                                    isRangeExist[currentBlock].Add(r, new Dictionary<int, bool>());

                                if (!selectedTextNumber[currentBlock].ContainsKey(r))
                                    selectedTextNumber[currentBlock].Add(r, new Dictionary<int, List<string>>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            selectedColumnNameCount++;
                                            columnName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                            filterConditionCount++;
                                        }
                                        if (bracket == "open")
                                        {
                                            if (compareOperatorDict.ContainsKey(ruleDetail[currentBlock][r].ToString().Substring(i, 2)))
                                            {
                                                if (ruleDetail[currentBlock][r].ToString().Substring(i, 2) == "..")
                                                {
                                                    if (!compareOperator[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                        compareOperator[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                    compareOperator[currentBlock][r][selectedColumnNameCount - 1].Add(">=");
                                                    compareOperator[currentBlock][r][selectedColumnNameCount - 1].Add("<=");

                                                    if (!isRangeExist[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                        isRangeExist[currentBlock][r].Add(selectedColumnNameCount - 1, true);

                                                    if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                        selectedTextNumber[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                    selectedTextNumber[currentBlock][r][selectedColumnNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                }
                                                else
                                                {
                                                    if (!compareOperator[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                        compareOperator[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                    compareOperator[currentBlock][r][selectedColumnNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, 2).Trim());
                                                }

                                                i++;
                                                startAddress = i;
                                            }
                                            else if (compareOperatorDict.ContainsKey(ruleDetail[currentBlock][r].ToString().Substring(i, 1)))
                                            {
                                                if (!compareOperator[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                    compareOperator[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                compareOperator[currentBlock][r][selectedColumnNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, 1).Trim());

                                                if (!isRangeExist[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                    isRangeExist[currentBlock][r].Add(selectedColumnNameCount - 1, false);

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                            {
                                                if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                    selectedTextNumber[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                selectedTextNumber[currentBlock][r][selectedColumnNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                                startAddress = i;
                                            }

                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                            {
                                                bracket = "close";

                                                if (!selectedTextNumber[currentBlock][r].ContainsKey(selectedColumnNameCount - 1))
                                                    selectedTextNumber[currentBlock][r].Add(selectedColumnNameCount - 1, new List<string>());

                                                selectedTextNumber[currentBlock][r][selectedColumnNameCount - 1].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int j = 0; j < columnName[currentBlock][r].Count; j++)
                                    {
                                        if (columnName[currentBlock][r].Count == selectedTextNumber[currentBlock][r].Count && columnName[currentBlock][r].Count == compareOperator[currentBlock][r].Count)
                                        {
                                            recognizedRule[currentBlock][r].Append(columnName[currentBlock][r][j] + "(");

                                            if (isRangeExist[currentBlock][r].ContainsKey(j))
                                            {
                                                if (isRangeExist[currentBlock][r][j] == true)
                                                    recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][0] + ".." + selectedTextNumber[currentBlock][r][j][1] + ")");

                                                if (isRangeExist[currentBlock][r][j] == false)
                                                {
                                                    for (int k = 0; k < compareOperator[currentBlock][r][j].Count; k++)
                                                    {
                                                        if (compareOperator[currentBlock][r][j].Count == selectedTextNumber[currentBlock][r][j].Count)
                                                        {
                                                            recognizedRule[currentBlock][r].Append(compareOperator[currentBlock][r][j][k]);

                                                            if (k < compareOperator[currentBlock][r][j].Count - 1)
                                                                recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ", ");
                                                            else
                                                                recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ")");
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                for (int k = 0; k < compareOperator[currentBlock][r][j].Count; k++)
                                                {
                                                    if (compareOperator[currentBlock][r][j].Count == selectedTextNumber[currentBlock][r][j].Count)
                                                    {
                                                        recognizedRule[currentBlock][r].Append(compareOperator[currentBlock][r][j][k]);

                                                        if (k < compareOperator[currentBlock][r][j].Count - 1)
                                                            recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ", ");
                                                        else
                                                            recognizedRule[currentBlock][r].Append(selectedTextNumber[currentBlock][r][j][k] + ")");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }


                            }
                            
                            // JoinTable
                            else if (joinTableTypeDict.ContainsKey(currentCommand))
                            {
                                if (!leftTableColumn[currentBlock].ContainsKey(r))
                                    leftTableColumn[currentBlock].Add(r, new List<string>());

                                if (!rightTableColumn[currentBlock].ContainsKey(r))
                                    rightTableColumn[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "@")
                                        {
                                            joinTableLeftRightSeparator++;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";

                                            if (joinTableLeftRightSeparator == 0)
                                                leftTable[currentBlock][r] = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if (joinTableLeftRightSeparator == 1)
                                                rightTable[currentBlock][r] = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "," || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                                bracket = "close";

                                            if (joinTableLeftRightSeparator == 0)
                                                leftTableColumn[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                            if (joinTableLeftRightSeparator == 1)
                                                rightTableColumn[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";

                                            if (convertTo == 1)
                                                resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                        }
                                    }
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    recognizedRule[currentBlock][r].Append(leftTable[currentBlock][r] + "(");

                                    for (int j = 0; j < leftTableColumn[currentBlock][r].Count; j++)
                                    {
                                        if (j < leftTableColumn[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(leftTableColumn[currentBlock][r][j] + ", ");
                                        else
                                            recognizedRule[currentBlock][r].Append(leftTableColumn[currentBlock][r][j] + ")");
                                    }

                                    recognizedRule[currentBlock][r].Append(" @ " + rightTable[currentBlock][r] + "(");

                                    for (int j = 0; j < rightTableColumn[currentBlock][r].Count; j++)
                                    {
                                        if (j < rightTableColumn[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(rightTableColumn[currentBlock][r][j] + ", ");
                                        else
                                            recognizedRule[currentBlock][r].Append(rightTableColumn[currentBlock][r][j] + ")");
                                    }

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");

                                    if (currentCommand == "INNERJOIN")
                                        joinTableType[currentBlock].Add(r, "InnerJoin");

                                    if (currentCommand == "FULLJOIN")
                                        joinTableType[currentBlock].Add(r, "FullJoin");

                                    if (currentCommand == "JOINTABLE")
                                        joinTableType[currentBlock].Add(r, "JoinTable");

                                    if (currentCommand == "CONDITIONALJOIN")
                                        joinTableType[currentBlock].Add(r, "ConditionalJoin");

                                    if (currentCommand == "RESIDUALJOIN")
                                        joinTableType[currentBlock].Add(r, "ResidualJoin");
                                }
                            }
                          
                            else if (currentCommand == "ORDERBY")
                            {
                                if (!orderByColumnName[currentBlock].ContainsKey(r))
                                    orderByColumnName[currentBlock].Add(r, new Dictionary<string, string>());

                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim().ToUpper();

                                            if (value == "A" || value == "D")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add(bracketName);
                                                orderByColumnName[currentBlock][r].Add(bracketName, value);
                                            }

                                            startAddress = i;
                                        }                                       

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)                                   
                                        recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + orderByColumnName[currentBlock][r][ruleTypeParameter[currentBlock][r][i]] + ")");                                                                         

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "REPLACERULE")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";

                                            startAddress = i;
                                        }
                                    }
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (newRule.Count == 0)
                                    {
                                        for (int j = 0; j < replaceRule.Count; j++)
                                        {
                                            if (j < replaceRule.Count - 1)
                                                recognizedRule[currentBlock][r].Append(replaceRule[j] + ", ");
                                            else
                                                if (resultTable[currentBlock].ContainsKey(r))
                                                recognizedRule[currentBlock][r].Append(replaceRule[j]);
                                            else
                                                recognizedRule[currentBlock][r].Append(replaceRule[j] + "}");
                                        }
                                    }
                                    else
                                    {
                                        for (int j = 0; j < replaceRule.Count; j++)
                                        {
                                            if (j < replaceRule.Count - 1)
                                                recognizedRule[currentBlock][r].Append(newRule[j] + ", ");
                                            else
                                                if (resultTable[currentBlock].ContainsKey(r))
                                                recognizedRule[currentBlock][r].Append(newRule[j]);
                                            else
                                                recognizedRule[currentBlock][r].Append(newRule[j] + "}");
                                        }
                                    }
                                }
                            }                          

                            else if (currentCommand == "VOUCHERENTRY")
                            {
                                if (!drCr[currentBlock].ContainsKey(r))
                                    drCr[currentBlock].Add(r, new List<string>());

                                if (!account[currentBlock].ContainsKey(r))
                                    account[currentBlock].Add(r, new List<string>());

                                if (!amount[currentBlock].ContainsKey(r))
                                    amount[currentBlock].Add(r, new List<string>());

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            
                                            var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if(debitDict.ContainsKey(bracketName.ToUpper()))
                                            {
                                               
                                                drCr[currentBlock][r].Add("Debit");
                                                account[currentBlock][r].Add(value);
                                                
                                            }

                                            else if (creditDict.ContainsKey(bracketName.ToUpper()))
                                            {   
                                                drCr[currentBlock][r].Add("Credit");
                                                account[currentBlock][r].Add(value);                                               
                                            }

                                            else if (balance.ContainsKey(bracketName.ToUpper()))
                                            {  
                                                drCr[currentBlock][r].Add("Balance");
                                                account[currentBlock][r].Add(value);
                                            }   
                                            
                                            else if(bracketName.ToUpper() == "EXCLUDEBALANCEGROUPBY")
                                            {
                                                drCr[currentBlock][r].Add("ExcludeBalanceGroupBy");
                                                account[currentBlock][r].Add(value);
                                            }

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "=" && ruleDetail[currentBlock][r].ToString().Substring(i + 1, 1) == ">")
                                        {
                                            arrow = true;
                                            startAddress = i + 1;
                                        }

                                        if (convertTo == 1)
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                                if (!resultTable[currentBlock].ContainsKey(r))
                                                    resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());

                                        if (arrow == true)
                                        {
                                            if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "," || ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                            {                                                
                                                var currentCell = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                amount[currentBlock][r].Add(currentCell);                                             
                                                startAddress = i;
                                            }

                                            if(convertTo == 0 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                            {
                                                var currentCell = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                                amount[currentBlock][r].Add(currentCell);
                                                startAddress = i;
                                            }
                                        }
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int i = 0; i < drCr[currentBlock][r].Count; i++)
                                       recognizedRule[currentBlock][r].Append(drCr[currentBlock][r][i] + "(" + account[currentBlock][r][i] + ")");

                                    recognizedRule[currentBlock][r].Append(" => ");

                                    for (int i = 0; i < amount[currentBlock][r].Count; i++)
                                    {
                                        if (i < amount[currentBlock][r].Count - 1)
                                            recognizedRule[currentBlock][r].Append(amount[currentBlock][r][i] + ",");
                                        else
                                            recognizedRule[currentBlock][r].Append(amount[currentBlock][r][i]);
                                    }

                                    if (resultTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(" ~ " + resultTable[currentBlock][r] + "}");
                                    else
                                        recognizedRule[currentBlock][r].Append("}");
                                }
                            }

                            else if (currentCommand == "CURRENTSQLSERVER" || currentCommand == "CURRENTCONNECTIONSTRING" || currentCommand == "RUNNONQUERYSQL")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open" && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                    {
                                        sqlStatement[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                        curlyBracket = "close";
                                    }
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");
                                    recognizedRule[currentBlock][r].Append(sqlStatement[currentBlock][r] + "}");
                                }
                            }

                            else if (currentCommand == "RUNSQL2DATATABLE" || currentCommand == "RUNSQL2LEDGERRAM")
                            {
                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            curlyBracket = "close";
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "~")
                                        {
                                            convertTo = 1;
                                            sqlStatement[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (convertTo == 1 && ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        {
                                            curlyBracket = "close";
                                            resultTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                        }
                                    }

                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");                                    
                                    recognizedRule[currentBlock][r].Append(sqlStatement[currentBlock][r] + " ~ " + resultTable[currentBlock][r] + "}");
                                }
                            }

                            else if (currentCommand == "LEDGERRAMAMEND2SQLSERVER")
                            {
                                if (!amendKey[currentBlock].ContainsKey(r))
                                    amendKey[currentBlock].Add(r, new List<string>());

                                if (!amendMode[currentBlock].ContainsKey(r))
                                    amendMode[currentBlock].Add(r, new List<string>());

                                if (!tableName[currentBlock].ContainsKey(r))
                                    tableName[currentBlock].Add(r, new List<string>());

                                if (!ruleTypeParameter[currentBlock].ContainsKey(r))
                                    ruleTypeParameter[currentBlock].Add(r, new List<string>());                               

                                for (int i = u; i < ruleDetail[currentBlock][r].Length; i++)
                                {
                                    if (curlyBracket == "open")
                                    {                                        
                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "|")
                                        {
                                            sourceTable[currentBlock].Add(r, ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "(")
                                        {
                                            bracket = "open";
                                            bracketName = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();
                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ")")
                                        {
                                            bracket = "close";
                                            var value = ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim();

                                            if (bracketName.ToUpper() == "AMENDKEY")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("AmendKey");
                                                amendKey[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }

                                            if (bracketName.ToUpper() == "AMENDMODE")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("AmendMode");
                                                amendMode[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }

                                            if (bracketName.ToUpper() == "TABLENAME")
                                            {
                                                ruleTypeParameter[currentBlock][r].Add("TableName");
                                                tableName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                            }

                                            startAddress = i;
                                        }

                                        if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == ",")
                                        {
                                            if (bracketName.ToUpper() == "AMENDKEY")
                                            {
                                                amendKey[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            if (bracketName.ToUpper() == "AMENDMODE")
                                            {
                                                amendMode[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                            if (bracketName.ToUpper() == "TABLENAME")
                                            {
                                                tableName[currentBlock][r].Add(ruleDetail[currentBlock][r].ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                                                startAddress = i;
                                            }

                                        }
                                       
                                    }

                                    if (ruleDetail[currentBlock][r].ToString().Substring(i, 1) == "}")
                                        curlyBracket = "close";
                                }

                                if (curlyBracket == "close")
                                {
                                    recognizedRule[currentBlock][r].Append(ruleType[currentBlock][r] + "{");

                                    if (sourceTable[currentBlock].ContainsKey(r))
                                        recognizedRule[currentBlock][r].Append(sourceTable[currentBlock][r] + " | ");

                                    for (int i = 0; i < ruleTypeParameter[currentBlock][r].Count; i++)
                                    {
                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "AMENDKEY")
                                        {
                                            for (int j = 0; j < amendKey[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + amendKey[currentBlock][r][j]);

                                                else if (j == amendKey[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + amendKey[currentBlock][r][j] + ") ");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + amendKey[currentBlock][r][j]);
                                            }

                                            if (amendKey[currentBlock][r].Count == 1)
                                                recognizedRule[currentBlock][r].Append(") ");
                                        }

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "AMENDMODE")
                                        {
                                            for (int j = 0; j < amendMode[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + amendMode[currentBlock][r][j]);

                                                else if (j == amendMode[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + amendMode[currentBlock][r][j] + ")");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + amendMode[currentBlock][r][j]);
                                            }

                                            if (amendMode[currentBlock][r].Count == 1)
                                                recognizedRule[currentBlock][r].Append(") ");
                                        }

                                        if (ruleTypeParameter[currentBlock][r][i].ToUpper() == "TABLENAME")
                                        {
                                            for (int j = 0; j < tableName[currentBlock][r].Count; j++)
                                            {
                                                if (j == 0)
                                                    recognizedRule[currentBlock][r].Append(ruleTypeParameter[currentBlock][r][i] + "(" + tableName[currentBlock][r][j]);

                                                else if (j == tableName[currentBlock][r].Count - 1)
                                                    recognizedRule[currentBlock][r].Append("," + tableName[currentBlock][r][j] + ")");

                                                else
                                                    recognizedRule[currentBlock][r].Append("," + tableName[currentBlock][r][j]);
                                            }

                                            if (tableName[currentBlock][r].Count == 1)
                                                recognizedRule[currentBlock][r].Append(") ");
                                        }
                                    }                                   

                                    recognizedRule[currentBlock][r].Append("}");
                                }
                            }
                        }                                               
                    }
                }

                if (ruleDetail[currentBlock].Count != ruleType[currentBlock].Count)
                {
                    message = ruleDetail[currentBlock].Count + " command found but only " + ruleType[currentBlock].Count + " are valid commands."; 
                    processMessage.Enqueue(message);                    

                    foreach (var pair in ruleType[currentBlock])
                    {
                        message = "Command " + (pair.Key + 1) + " " + pair.Value + Environment.NewLine + Environment.NewLine;
                        processMessage.Enqueue(message);                        
                    }

                    isExecute = false;
                }

                if (ruleDetail[currentBlock].Count == ruleType[currentBlock].Count)
                {                    
                    for (int i = 0; i < recognizedRule[currentBlock].Count; i++)
                    {  
                        var sourceRule = ruleDetail[currentBlock][i].ToString().ToUpper().Replace(" ", "");
                        var recognizeRule = recognizedRule[currentBlock][i].ToString().ToUpper().Replace(" ", "");
                        if (!ruleDetail[currentBlock][i].ToString().Contains("//"))
                        {
                            if (sourceRule != recognizeRule)
                            {
                                message = Environment.NewLine + "sourceRule:    " + "Rule " + (i + 1) + " " + ruleDetail[currentBlock][i] + Environment.NewLine + "recognizeRule: " + "Rule " + (i + 1) + " " + recognizedRule[currentBlock][i] + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }
                    }                   
                }

                if (isExecute == true)
                {
                    message = null;
                    message = null;
                    // Execute Rules         
                    Dictionary<string, int> upperColumnName2ID;                   
                    DataTable _dataTable = new DataTable();
                    string currentTable = null;
                    string currentSQLServer = null;
                    string currentConnectionString = null;
                    string currentCell = null;
                    string currentCellValue = null;
                    Dictionary<string, double> cellNumberStore = new Dictionary<string, double>();
                    Dictionary<string, string> cellTextStore = new Dictionary<string, string>();
                    isExecute = true;
                    // int rule = 0;
                    DateTime endTime = DateTime.Now;

                    message = string.Format("Read Rule:" + currentSetting.calcRule + " Time = {0:0.000}", (endTime - startTime).TotalSeconds) + "s" + Environment.NewLine; 
                    processMessage.Enqueue(message);

                    TimeSpan diffTime;
                    ConcurrentDictionary<int, Thread> loopBlockThread = new ConcurrentDictionary<int, Thread>();
                    ConcurrentQueue<string> looprunCurrentBlock = new ConcurrentQueue<string>();
                    ConcurrentQueue<string> completinglooprunCurrentBlock = new ConcurrentQueue<string>();
                    ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
                    ConcurrentDictionary<int, ruleProcessing> writeColumnThread = new ConcurrentDictionary<int, ruleProcessing>();                   
                    List<bool> isExit = new List<bool>();
                    StringBuilder sqlCommand = new StringBuilder();
                    isExit.Add(false);          

                    Dictionary<string, Action<string, int>> commandDict = new Dictionary<string, Action<string, int>>();
                    commandDict.Add("AMENDCOLUMNNAME", AMENDCOLUMNNAME);
                    commandDict.Add("AMENDDATE", AMENDDATE);
                    commandDict.Add("REVERSEMONTHLYVOUCHER", AMENDDATE);
                    commandDict.Add("REVERSEDAILYVOUCHER", AMENDDATE);
                    commandDict.Add("REVERSEWEEKLYVOUCHER", AMENDDATE);
                    commandDict.Add("AMENDDATEFORMAT", AMENDDATEFORMAT);
                    commandDict.Add("AMORTIZATION", AMORTIZATION);
                    commandDict.Add("APPENDROW", APPENDROW);
                    commandDict.Add("BUILDDAILYBALANCE", BUILDBALANCE);
                    commandDict.Add("BUILDDAILYBALANCECROSSTABPERIOD", BUILDBALANCE);
                    commandDict.Add("BUILDMONTHLYBALANCE", BUILDBALANCE);
                    commandDict.Add("BUILDMONTHLYBALANCECROSSTABPERIOD", BUILDBALANCE);
                    commandDict.Add("BUILDWEEKLYBALANCE", BUILDBALANCE);
                    commandDict.Add("BUILDWEEKLYBALANCECROSSTABPERIOD", BUILDBALANCE);
                    commandDict.Add("COMPUTECELL", COMPUTECELL);
                    commandDict.Add("COMPUTECOLUMN", COMPUTECOLUMN);
                    commandDict.Add("ANDCONDITION2ACTION", CONDITION);
                    commandDict.Add("ORCONDITION2ACTION", CONDITION);
                    commandDict.Add("ANDCONDITION2CELL", CONDITION);
                    commandDict.Add("ORCONDITION2CELL", CONDITION);
                    commandDict.Add("COPYTABLE", COPYTABLE);
                    commandDict.Add("COPYTABLE.COMMONTABLE", COPYTABLECOMMONTABLE);
                    commandDict.Add("CROSSTAB", CROSSTAB);
                    commandDict.Add("CROSSTABSQLTABLE", CROSSTAB);
                    commandDict.Add("CSV2LEDGERRAM", CSV2LEDGERRAM);
                    commandDict.Add("TEXT2LEDGERRAM", CSV2LEDGERRAM);
                    commandDict.Add("ONECOLUMN2LEDGERRAM", CSV2LEDGERRAM);
                    commandDict.Add("CSV2LEDGERRAM.COMMONTABLE", CSV2LEDGERRAMCOMMONTABLE);
                    commandDict.Add("CONTINUEPROCESS", CONTINUEPROCESS);
                    commandDict.Add("CURRENTTABLE", CURRENTTABLE);
                    commandDict.Add("CURRENTBUILDBALANCESETTING", CURRENTBUILDBALANCESETTING);
                    commandDict.Add("DATATABLE2LEDGERRAM", DATATABLE2LEDGERRAM);
                    commandDict.Add("DATE2EFFECTIVEDATE", DATE2EFFECTIVEDATE);
                    commandDict.Add("DATE2MONTHLYPERIOD", DATE2PERIOD);
                    commandDict.Add("DATE2DAILYPERIOD", DATE2PERIOD);
                    commandDict.Add("DATE2WEEKLYPERIOD", DATE2PERIOD);
                    commandDict.Add("DC2POSITIVENEGATIVE", DC);
                    commandDict.Add("DC2NEGATIVEPOSITIVE", DC);
                    commandDict.Add("DISABLE", DISABLE);
                    commandDict.Add("DISTINCT", DISTINCT);
                    commandDict.Add("ENABLE", ENABLE);
                    commandDict.Add("ENDPROCESS", ENDPROCESS);
                    commandDict.Add("ANDFILTER", FILTER);
                    commandDict.Add("ORFILTER", FILTER);
                    commandDict.Add("GROUPBY", GROUPBY);
                    commandDict.Add("FILELIST2LEDGERRAM", FILELIST2LEDGERRAM);
                    commandDict.Add("ANDFILTER.DISTINCTLIST", FILTERDISTINCTLIST);
                    commandDict.Add("ORFILTER.DISTINCTLIST", FILTERDISTINCTLIST);
                    commandDict.Add("ANDFILTER.CONDITIONLIST", FILTERCONDITIONLIST);
                    commandDict.Add("ORFILTER.CONDITIONLIST", FILTERCONDITIONLIST);
                    commandDict.Add("INNERJOIN", JOINTABLE);
                    commandDict.Add("FULLJOIN", JOINTABLE);
                    commandDict.Add("JOINTABLE", JOINTABLE);
                    commandDict.Add("CONDITIONALJOIN", JOINTABLEBYCONDITION);
                    commandDict.Add("RESIDUALJOIN", JOINTABLEBYCONDITION);
                    commandDict.Add("LEDGERRAM2CSV", LEDGERRAM2CSV);
                    commandDict.Add("LEDGERRAM2TEXT", LEDGERRAM2CSV);
                    commandDict.Add("LEDGERRAM2ONECOLUMN", LEDGERRAM2CSV);
                    commandDict.Add("LEDGERRAM2DATATABLE", LEDGERRAM2DATATABLE);
                    commandDict.Add("LEDGERRAM2HTML", LEDGERRAM2HTML);
                    commandDict.Add("LEDGERRAM2JSON", LEDGERRAM2JSON);
                    commandDict.Add("LEDGERRAM2XML", LEDGERRAM2XML);
                    commandDict.Add("MANYCSV2LEDGERRAM", MANYCSV2LEDGERRAM);
                    commandDict.Add("REVERSEMANYCROSSTABCSV2LEDGERRAM", MANYCSV2LEDGERRAM);                    
                    commandDict.Add("MERGETABLE", MERGETABLE);
                    commandDict.Add("MERGECOMMONTABLE", MERGETABLE);
                    commandDict.Add("COMBINETABLEBYCOMMONCOLUMN", MERGETABLE);
                    commandDict.Add("NEGATIVEPOSITIVE2DC", DC);
                    commandDict.Add("NUMBER2TEXT", NUMBER2TEXT);
                    commandDict.Add("ORDERBY", ORDERBY);
                    commandDict.Add("POSITIVENEGATIVE2DC", DC);
                    commandDict.Add("PROCESS", PROCESS);
                    commandDict.Add("REPLACERULE", REPLACERULE);
                    commandDict.Add("REVERSECROSSTAB", REVERSECROSSTAB);
                    commandDict.Add("REVERSECROSSTABCSV2LEDGERRAM", REVERSECROSSTABCSV2LEDGERRAM);
                    commandDict.Add("REVERSEDC", REVERSEDC);
                    commandDict.Add("REVERSENUMBER", REVERSENUMBER);
                    commandDict.Add("RULE2LEDGERRAM", RULE2LEDGERRAM);
                    commandDict.Add("SELECTCOLUMN", SELECTCOLUMN);
                    commandDict.Add("REMOVECOLUMN", SELECTCOLUMN);
                    commandDict.Add("TABLE2CELL", TABLE2CELL);
                    commandDict.Add("VOUCHERENTRY", VOUCHERENTRY);
                    commandDict.Add("PARALLELPROCESS", PARALLELPROCESS);
                    commandDict.Add("CURRENTSQLSERVER", CURRENTSQLSERVER);
                    commandDict.Add("CURRENTCONNECTIONSTRING", CURRENTCONNECTIONSTRING);
                    commandDict.Add("LEDGERRAMCLONE2SQLSERVER", LEDGERRAM2SQLSERVER);
                    commandDict.Add("LEDGERRAMAPPEND2SQLSERVER", LEDGERRAM2SQLSERVER);
                    commandDict.Add("DATATABLECLONE2SQLSERVER", DATATABLE2SQLSERVER);
                    commandDict.Add("DATATABLEAPPEND2SQLSERVER", DATATABLE2SQLSERVER);
                    commandDict.Add("RUNSQL2DATATABLE", SQL2DATATABLE);
                    commandDict.Add("RUNSQL2LEDGERRAM", SQL2LEDGERRAM);
                    commandDict.Add("SQLTABLE2LEDGERRAM", SQL2LEDGERRAM);
                    commandDict.Add("DISTINCTSQLTABLE", SQL2LEDGERRAM);
                    commandDict.Add("GROUPSQLTABLEBY", GROUPSQLTABLEBY);
                    commandDict.Add("CREATESQLDATABASE", CREATESQLDATABASE);
                    commandDict.Add("REMOVESQLDATABASE", REMOVESQLDATABASE);
                    commandDict.Add("RUNNONQUERYSQL", RUNNONQUERYSQL);
                    commandDict.Add("FILTERSQLROW.ANDCONDITION", FILTERSQLROW);
                    commandDict.Add("FILTERSQLROW.ORCONDITION", FILTERSQLROW);
                    commandDict.Add("REMOVESQLROW.ANDCONDITION", FILTERSQLROW);
                    commandDict.Add("REMOVESQLROW.ORCONDITION", FILTERSQLROW);
                    commandDict.Add("FILTERSQLROW.ANDCONDITIONTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("FILTERSQLROW.ORCONDITIONTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("REMOVESQLROW.ANDCONDITIONTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("REMOVESQLROW.ORCONDITIONTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("FILTERSQLROW.ANDDISTINCTTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("FILTERSQLROW.ORDISTINCTTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("REMOVESQLROW.ANDDISTINCTTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("REMOVESQLROW.ORDISTINCTTABLE", FILTERSQLROWBYCONDITIONTABLE);
                    commandDict.Add("LEDGERRAMAMEND2SQLSERVER", LEDGERRAMAMEND2SQLSERVER);
                    commandDict.Add("REMOVESQLTABLE", REMOVESQLTABLE);
                    commandDict.Add("REMOVESQLCOLUMN", REMOVESQLCOLUMN);                    


                    runBlock("Main");                   

                    void runBlock(string runCurrentBlock)
                    {
                        int i = 0;
                        int ii = 0;

                        do
                        {
                            if (isExecute == false)
                                break;                          

                            if (ruleType.ContainsKey(runCurrentBlock))
                            {
                                if (ruleType[runCurrentBlock][i].ToUpper().Trim() == "ENDPROCESS")
                                {
                                    commandDict[ruleType[runCurrentBlock][i].ToUpper().Trim()](runCurrentBlock, i);
                                    processMessage.Enqueue(Environment.NewLine + "Process Completed");
                                    break;
                                }

                                if (ruleType[runCurrentBlock][i].ToUpper().Trim() == "CONTINUEPROCESS")
                                {
                                    runCurrentBlock = columnName[runCurrentBlock][i][0];

                                    do
                                    {
                                        i = 0;

                                        do
                                        {
                                            commandDict[ruleType[runCurrentBlock][i].ToUpper().Trim()](runCurrentBlock, i);

                                            i++;
                                        } while (i < ruleDetail[runCurrentBlock].Count);

                                        ii++;

                                    } while (isExit[0] == false);
                                }
                                else if (ruleType[runCurrentBlock][i].ToUpper().Trim() == "PROCESS")
                                    commandDict[ruleType[runCurrentBlock][i].ToUpper().Trim()](runCurrentBlock, i);
                                else                                                               
                                    commandDict[ruleType[runCurrentBlock][i].ToUpper().Trim()](runCurrentBlock, i);                              
                            }

                            i++;
                        } while (i < ruleDetail[runCurrentBlock].Count);                      
                    }

                    void LEDGERRAMAMEND2SQLSERVER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        distinct newDistinct = new distinct();
                        dinstinctSetting setDistinct = new dinstinctSetting();
                        setDistinct.distinctColumnName = amendKey[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }                        

                        SQL newFilter = new SQL();
                        SQLsetting setFilter = new SQLsetting();
                        setFilter.currentSQLServer = currentSQLServer;
                        setFilter.currentConnectionString = currentConnectionString;
                        setFilter.filterType = "And";                        
                        setFilter.command = "REMOVESQLROW.ANDCONDITIONTABLE";
                        setFilter.sourceTable = tableName[runCurrentBlock][i][0];
                        setFilter.selectedColumnName = amendKey[runCurrentBlock][i]; 

                        if (isExecute == true)
                        {                          
                            (ramStore["None"], classMessage) = newFilter.filterSQL2LedgerRAMByConditionTable(newDistinct.distinctList(ramStore[currentTable], setDistinct), setFilter);

                            if (classMessage.Contains("Fail"))
                            {
                                tableSizeMessage[runCurrentBlock][i] = classMessage;
                                isExecute = false;
                            }
                            else if (classMessage.Contains("Filtered"))
                            {
                                tableSizeMessage[runCurrentBlock][i] = classMessage;
                            }
                            else
                                OutputCurrentTableMessage(runCurrentBlock, i);                           
                        }

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;                                                
                        setSQL.updateSQLtype = "LEDGERRAMAPPEND2SQLSERVER";
                        setSQL.resultTable = tableName[runCurrentBlock][i][0];                       

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            setSQL.sourceTable = sourceTable[runCurrentBlock][i];

                        classMessage = currentSQL.LedgerRAM2SQL(ramStore[currentTable], setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock][i] = classMessage;

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CREATESQLDATABASE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;                        
                        setSQL.resultDatabase = columnName[runCurrentBlock][i][0];

                        classMessage = currentSQL.createDatabase(setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void REMOVESQLDATABASE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                        setSQL.resultDatabase = columnName[runCurrentBlock][i][0];

                        classMessage = currentSQL.removeDatabase(setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void REMOVESQLTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                        setSQL.resultTable = columnName[runCurrentBlock][i][0];

                        classMessage = currentSQL.removeTable(setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void REMOVESQLCOLUMN(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                        setSQL.resultColumn = columnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            setSQL.resultTable = sourceTable[runCurrentBlock][i];

                        classMessage = currentSQL.removeColumn(setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void RUNNONQUERYSQL(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                        setSQL.sqlStatement = sqlStatement[runCurrentBlock][i];

                        classMessage = currentSQL.runNonQuerySQL(setSQL);
                        
                        if(classMessage != null)                        
                            if (classMessage.Contains("Fail"))
                                isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void FILTERSQLROW(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        SQL newFilter = new SQL();
                        SQLsetting setFilter = new SQLsetting();
                        setFilter.currentSQLServer = currentSQLServer;
                        setFilter.currentConnectionString = currentConnectionString;
                        setFilter.filterType = filterType[runCurrentBlock][i];
                        setFilter.selectedColumnName = columnName[runCurrentBlock][i];
                        setFilter.compareOperator = compareOperator[runCurrentBlock][i];
                        setFilter.selectedTextNumber = selectedTextNumber[runCurrentBlock][i];
                        setFilter.command = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            setFilter.sourceTable = sourceTable[runCurrentBlock][i];

                        if (selectedText[runCurrentBlock].Count > 0)
                            setFilter.selectedText = selectedText[runCurrentBlock][i];

                        if (selectedNumber[runCurrentBlock].Count > 0)
                            setFilter.selectedNumber = selectedNumber[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        if (isExecute == true)
                        {                           
                            (ramStore[resultTable[runCurrentBlock][i]], classMessage) = newFilter.oneRowfilterSQL2LedgerRAM(setFilter);
                            currentTable = resultTable[runCurrentBlock][i];

                            if (classMessage.Contains("Fail"))
                            {
                                tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                                isExecute = false;
                            }
                            else if (classMessage.Contains("Filtered"))
                            {
                                tableSizeMessage[runCurrentBlock].Add(i, classMessage);                                   
                            }
                            else
                                OutputCurrentTableMessage(runCurrentBlock, i);                            
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void FILTERSQLROWBYCONDITIONTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        SQL newFilter = new SQL();
                        SQLsetting setFilter = new SQLsetting();
                        setFilter.currentSQLServer = currentSQLServer;
                        setFilter.currentConnectionString = currentConnectionString;                                               
                        setFilter.command = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if(amendKey[runCurrentBlock].ContainsKey(i))
                            setFilter.selectedColumnName = amendKey[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "FILTERSQLROW.ANDCONDITIONTABLE" || currentCommand == "REMOVESQLROW.ANDCONDITIONTABLE" || currentCommand == "FILTERSQLROW.ANDDISTINCTTABLE" || currentCommand == "REMOVESQLROW.ANDDISTINCTTABLE")
                            setFilter.filterType = "And";

                        if (currentCommand == "FILTERSQLROW.ORCONDITIONTABLE" || currentCommand == "REMOVESQLROW.ORCONDITIONTABLE" || currentCommand == "FILTERSQLROW.ORDISTINCTTABLE" || currentCommand == "REMOVESQLROW.ORDISTINCTTABLE")
                            setFilter.filterType = "Or";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            setFilter.sourceTable = sourceTable[runCurrentBlock][i];                        

                        if (isExecute == true)
                        {                           
                            (ramStore[resultTable[runCurrentBlock][i]], classMessage) = newFilter.filterSQL2LedgerRAMByConditionTable(ramStore[masterTable[runCurrentBlock][i]], setFilter);
                            currentTable = resultTable[runCurrentBlock][i];

                            if (classMessage.Contains("Fail"))
                            {
                                tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                                isExecute = false;
                            }
                            else if (classMessage.Contains("Filtered"))
                            {
                                tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                            }
                            else
                                OutputCurrentTableMessage(runCurrentBlock, i);                         
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CURRENTSQLSERVER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        currentSQLServer = sqlStatement[runCurrentBlock][i];
                        //OutputCurrentTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CURRENTCONNECTIONSTRING(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        currentConnectionString = sqlStatement[runCurrentBlock][i];
                        //OutputCurrentTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void LEDGERRAM2SQLSERVER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);                      

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                        setSQL.sourceTable = currentTable;
                        setSQL.resultTable = resultTable[runCurrentBlock][i];
                        setSQL.updateSQLtype = ruleType[runCurrentBlock][i].ToUpper().Trim();                        

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        classMessage = currentSQL.LedgerRAM2SQL(ramStore[currentTable], setSQL);

                        if (classMessage.Contains("Fail"))                       
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                        
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void DATATABLE2SQLSERVER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        SQL currentSQL = new SQL();

                        SQLsetting setSQL = new SQLsetting();
                        setSQL.currentSQLServer = currentSQLServer;
                        setSQL.currentConnectionString = currentConnectionString;
                       
                        setSQL.resultTable = resultTable[runCurrentBlock][i];
                        setSQL.updateSQLtype = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        classMessage = currentSQL.DataTable2SQL(_dataTable, setSQL);

                        if (classMessage.Contains("Fail"))
                            isExecute = false;

                        tableSizeMessage[runCurrentBlock].Add(i, classMessage);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void SQL2DATATABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        
                        printProcessRuleLog(runCurrentBlock, i);
                        SQL newSQL = new SQL();
                        SQLsetting setSQLsetting = new SQLsetting();
                        setSQLsetting.currentSQLServer = currentSQLServer;
                        setSQLsetting.currentConnectionString = currentConnectionString;
                        setSQLsetting.sqlStatement = sqlStatement[runCurrentBlock][i];
                        setSQLsetting.resultTable = resultTable[runCurrentBlock][i];

                        (_dataTable, classMessage) = newSQL.SQL2DataTable(setSQLsetting);                       

                        if (classMessage.Contains("Fail"))
                        {
                            tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                            isExecute = false;
                        }                        
                        else
                            OutputDataTableMessage(runCurrentBlock, i);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void SQL2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;

                        printProcessRuleLog(runCurrentBlock, i);
                        SQL newSQL = new SQL();
                        SQLsetting setSQLsetting = new SQLsetting();
                        setSQLsetting.currentSQLServer = currentSQLServer;
                        setSQLsetting.currentConnectionString = currentConnectionString;
                        setSQLsetting.resultTable = resultTable[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();                     

                        if (sourceTable[runCurrentBlock].ContainsKey(i) && currentCommand == "SQLTABLE2LEDGERRAM")
                        {
                            if(columnName[runCurrentBlock][i][0] == "*")
                                setSQLsetting.sqlStatement = "Select * from " + sourceTable[runCurrentBlock][i] + ";";
                            else
                            {
                                sqlCommand.Append("Select ");

                                for (int j = 0; j < columnName[runCurrentBlock][i].Count; j++)
                                {
                                    if (j < columnName[runCurrentBlock][i].Count - 1)
                                        sqlCommand.Append(columnName[runCurrentBlock][i][j].Replace(" ","_").Replace("/","") + ",");
                                    else
                                        sqlCommand.Append(columnName[runCurrentBlock][i][j].Replace(" ", "_").Replace("/", ""));
                                }

                                sqlCommand.Append(" from " + sourceTable[runCurrentBlock][i] + ";");

                                setSQLsetting.sqlStatement = sqlCommand.ToString();
                            }
                        }

                        if (sourceTable[runCurrentBlock].ContainsKey(i) && currentCommand == "DISTINCTSQLTABLE")
                        {                            
                            sqlCommand.Append("Select Distinct ");

                            for (int j = 0; j < columnName[runCurrentBlock][i].Count; j++)
                            {
                                if (j < columnName[runCurrentBlock][i].Count - 1)
                                    sqlCommand.Append(columnName[runCurrentBlock][i][j].Replace(" ", "_").Replace("/", "") + ",");
                                else
                                    sqlCommand.Append(columnName[runCurrentBlock][i][j].Replace(" ", "_").Replace("/", ""));
                            }

                            sqlCommand.Append(" from " + sourceTable[runCurrentBlock][i] + ";");

                            setSQLsetting.sqlStatement = sqlCommand.ToString();

                            // Console.WriteLine(setSQLsetting.sqlStatement);
                        }

                        if (currentCommand == "RUNSQL2LEDGERRAM")
                            setSQLsetting.sqlStatement = sqlStatement[runCurrentBlock][i];                        

                        (ramStore[resultTable[runCurrentBlock][i]], classMessage) = newSQL.SQL2LedgerRAM(setSQLsetting);
                        currentTable = resultTable[runCurrentBlock][i];

                        if (classMessage.Contains("Fail"))
                        {
                            tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                            isExecute = false;
                        }
                        else
                          OutputResultTableMessage(runCurrentBlock, i);

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void GROUPSQLTABLEBY(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;

                        printProcessRuleLog(runCurrentBlock, i);
                      
                        if (sourceTable[runCurrentBlock].ContainsKey(i) && resultTable[runCurrentBlock].ContainsKey(i))
                        {
                            SQL newSQL = new SQL();
                            SQLsetting setSQLsetting = new SQLsetting();
                            setSQLsetting.currentSQLServer = currentSQLServer;
                            setSQLsetting.currentConnectionString = currentConnectionString;
                            setSQLsetting.resultTable = resultTable[runCurrentBlock][i];
                            setSQLsetting.groupByColumnName = columnName[runCurrentBlock][i];
                            setSQLsetting.aggregateFunction = aggregateFunction[runCurrentBlock][i];
                            setSQLsetting.aggregateByColumnName = aggregateByColumnName[runCurrentBlock][i];
                            setSQLsetting.selectedColumnName = columnName[runCurrentBlock][i];
                            setSQLsetting.sourceTable = sourceTable[runCurrentBlock][i];                            

                            (ramStore[resultTable[runCurrentBlock][i]], classMessage) = newSQL.groupSQLTableBy(setSQLsetting);
                            currentTable = resultTable[runCurrentBlock][i];

                            if (classMessage.Contains("Fail"))
                            {
                                tableSizeMessage[runCurrentBlock].Add(i, classMessage);
                                isExecute = false;
                            }
                            else
                                OutputResultTableMessage(runCurrentBlock, i);
                        }
                        else 
                          isExecute = false;
                       
                        diffTime = DateTime.Now - startTime;

                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void AMENDCOLUMNNAME(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        amendColumnName newAmendColumnName = new amendColumnName();
                        amendColumnNameSetting setAmendColumnName = new amendColumnNameSetting();
                        setAmendColumnName.sourceColumnName = sourceColumnName[runCurrentBlock][i];
                        setAmendColumnName.resultColumnName = resultColumnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        if (upperColumnName2ID.ContainsKey(resultColumnName[runCurrentBlock][i].ToUpper()))
                        {
                            message = Environment.NewLine + "       \"" + resultColumnName[runCurrentBlock][i] + "\" duplicate with existing column name of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                            processMessage.Enqueue(message);
                            isExecute = false;
                        }

                        if (!upperColumnName2ID.ContainsKey(sourceColumnName[runCurrentBlock][i].ToUpper()))
                        {
                            message = Environment.NewLine + "       \"" + sourceColumnName[runCurrentBlock][i] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                            processMessage.Enqueue(message);
                            isExecute = false;
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newAmendColumnName.amendColumnNameProcess(ramStore[currentTable], setAmendColumnName);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newAmendColumnName.amendColumnNameProcess(ramStore[currentTable], setAmendColumnName);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void AMENDDATE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        amendDate newAmendDate = new amendDate();
                        amendDateSetting setAmendDate = new amendDateSetting();
                        setAmendDate.ruleType = ruleType[runCurrentBlock][i].ToUpper().Trim();
                        setAmendDate.columnName = columnName[runCurrentBlock][i];

                        if (!cultureOption[runCurrentBlock].ContainsKey(i))
                            setAmendDate.cultureOption = "zh-HK";
                        else
                            setAmendDate.cultureOption = cultureOption[runCurrentBlock][i];

                        if (currentCommand == "REVERSEMONTHLYVOUCHER")
                            setAmendDate.periodType = "MONTH";                      

                        if (currentCommand == "REVERSEDAILYVOUCHER")
                            setAmendDate.periodType = "DAY";

                        if (currentCommand == "REVERSEWEEKLYVOUCHER")
                            setAmendDate.periodType = "WEEK";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        if (addDay[runCurrentBlock].ContainsKey(i))
                            setAmendDate.addDay = addDay[runCurrentBlock][i];
                        else
                            setAmendDate.addDay = 0;

                        if (addMonth[runCurrentBlock].ContainsKey(i))
                            setAmendDate.addMonth = addMonth[runCurrentBlock][i];
                        else
                            setAmendDate.addMonth = 0;

                        if (addYear[runCurrentBlock].ContainsKey(i))
                            setAmendDate.addYear = addYear[runCurrentBlock][i];
                        else
                            setAmendDate.addYear = 0;

                        if (day[runCurrentBlock].ContainsKey(i))
                            setAmendDate.day = day[runCurrentBlock][i];
                        else
                            setAmendDate.day = 0;

                        if (month[runCurrentBlock].ContainsKey(i))
                            setAmendDate.month = month[runCurrentBlock][i];
                        else
                            setAmendDate.month = 0;

                        if (year[runCurrentBlock].ContainsKey(i))
                            setAmendDate.year = year[runCurrentBlock][i];
                        else
                            setAmendDate.year = 0;

                        if (startMonth[runCurrentBlock].ContainsKey(i))
                            setAmendDate.startMonth = startMonth[runCurrentBlock][i];
                        else
                            setAmendDate.startMonth = 0;

                        if (startDay[runCurrentBlock].ContainsKey(i))
                            setAmendDate.startDay = startDay[runCurrentBlock][i];
                        else
                            setAmendDate.startDay = 0;

                        if (startWeek[runCurrentBlock].ContainsKey(i))
                            setAmendDate.startWeek = startWeek[runCurrentBlock][i];
                        else
                            setAmendDate.startWeek = 0;

                        if (nextPeriodAddDay[runCurrentBlock].ContainsKey(i))
                            setAmendDate.nextPeriodAddDay = nextPeriodAddDay[runCurrentBlock][i];
                        else
                            setAmendDate.nextPeriodAddDay = -999;

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        Dictionary<string, string> upperCellTextStore = new Dictionary<string, string>();

                        foreach (var pair in cellTextStore)
                            upperCellTextStore.Add(pair.Key.ToUpper(), pair.Value);

                        Dictionary<string, string> upperRamStore = new Dictionary<string, string>();

                        foreach (var pair in ramStore)
                            upperRamStore.Add(pair.Key.ToUpper(), pair.Key.ToUpper());

                        isExecute = false;

                        if (columnName[runCurrentBlock][i][0] == "*")
                        {
                            isExecute = true;

                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newAmendDate.amendDateByTable(ramStore[currentTable], setAmendDate);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newAmendDate.amendDateByTable(ramStore[currentTable], setAmendDate);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        else if (upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][0].ToUpper()))
                        {
                            isExecute = true;

                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                if (masterTable[runCurrentBlock].ContainsKey(i))
                                    ramStore[currentTable] = newAmendDate.amendDateByColumn(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setAmendDate);
                                else
                                    ramStore[currentTable] = newAmendDate.amendDateByColumn(ramStore[currentTable], null, setAmendDate);

                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                if (masterTable[runCurrentBlock].ContainsKey(i))
                                    ramStore[resultTable[runCurrentBlock][i]] = newAmendDate.amendDateByColumn(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setAmendDate);
                                else
                                    ramStore[resultTable[runCurrentBlock][i]] = newAmendDate.amendDateByColumn(ramStore[currentTable], null, setAmendDate);

                                currentTable = resultTable[runCurrentBlock][i];

                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        else if (upperCellTextStore.ContainsKey(columnName[runCurrentBlock][i][0].ToUpper()))
                        {
                            isExecute = true;

                            currentCell = resultCell[runCurrentBlock][i];

                            var currentText = newAmendDate.amendDateByCell(upperCellTextStore[columnName[runCurrentBlock][i][0].ToUpper()], setAmendDate);

                            if (cellTextStore.ContainsKey(currentCell))
                                cellTextStore.Remove(currentCell);

                            cellTextStore.Add(currentCell, currentText);
                            currentCellValue = cellTextStore[currentCell];

                            OutputResultCellMessage(runCurrentBlock, i);
                        }

                        if (isExecute == false)
                        {
                            message = Environment.NewLine + "       Fail to find data from " + "\"" + setAmendDate.sourceDataName + "\".";
                            processMessage.Enqueue(message);
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void AMENDDATEFORMAT(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        amendDateFormat newAmendDateFormat = new amendDateFormat();
                        amendDateFormatSetting setAmendDateFormat = new amendDateFormatSetting();
                        setAmendDateFormat.columnName = columnName[runCurrentBlock][i];
                        setAmendDateFormat.sourceDateFormat = sourceDateFormat[runCurrentBlock][i];
                        setAmendDateFormat.resultDateFormat = resultDateFormat[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        Dictionary<string, string> upperCellTextStore = new Dictionary<string, string>();

                        foreach (var pair in cellTextStore)
                            upperCellTextStore.Add(pair.Key.ToUpper(), pair.Value);

                        Dictionary<string, string> upperRamStore = new Dictionary<string, string>();

                        foreach (var pair in ramStore)
                            upperRamStore.Add(pair.Key.ToUpper(), pair.Key.ToUpper());

                        isExecute = false;

                        if (columnName[runCurrentBlock][i][0] == "*")
                        {
                            isExecute = true;

                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newAmendDateFormat.amendDateFormatByTable(ramStore[currentTable], setAmendDateFormat);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newAmendDateFormat.amendDateFormatByTable(ramStore[currentTable], setAmendDateFormat);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }
                        else if (upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][0].ToUpper()))
                        {
                            isExecute = true;

                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newAmendDateFormat.amendDateFormatByColumn(ramStore[currentTable], setAmendDateFormat);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newAmendDateFormat.amendDateFormatByColumn(ramStore[currentTable], setAmendDateFormat);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }
                        else if (upperCellTextStore.ContainsKey(columnName[runCurrentBlock][i][0].ToUpper()))
                        {
                            isExecute = true;

                            currentCell = resultCell[runCurrentBlock][i];

                            var currentText = newAmendDateFormat.amendDateFormatByCell(upperCellTextStore[columnName[runCurrentBlock][i][0].ToUpper()], setAmendDateFormat);

                            if (cellTextStore.ContainsKey(currentCell))
                                cellTextStore.Remove(currentCell);

                            cellTextStore.Add(currentCell, currentText);
                            currentCellValue = cellTextStore[currentCell];

                            OutputResultCellMessage(runCurrentBlock, i);
                        }

                        if (isExecute == false)
                        {
                            message = Environment.NewLine + "       Fail to find data from " + "\"" + setAmendDateFormat.sourceDataName + "\".";
                            processMessage.Enqueue(message);
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void AMORTIZATION(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        amortization newAmortization = new amortization();
                        amortizationSetting setAmortization = new amortizationSetting();
                        bool isNumber;

                        if (amortizationMethod[runCurrentBlock].ContainsKey(i))
                            setAmortization.amortizationMethod = amortizationMethod[runCurrentBlock][i];

                        if (assetID[runCurrentBlock].ContainsKey(i))
                            setAmortization.assetID = assetID[runCurrentBlock][i];

                        if (acquisition[runCurrentBlock].ContainsKey(i))
                        {
                            isNumber = double.TryParse(acquisition[runCurrentBlock][i], out double number);

                            if (isNumber == true)
                                setAmortization.acquisition = number;
                        }

                        if (residual[runCurrentBlock].ContainsKey(i))
                        {
                            isNumber = double.TryParse(residual[runCurrentBlock][i], out double number);

                            if (isNumber == true)
                                setAmortization.residual = number;
                        }

                        if (totalTenor[runCurrentBlock].ContainsKey(i))
                        {
                            isNumber = double.TryParse(totalTenor[runCurrentBlock][i], out double number);

                            if (isNumber == true)
                                setAmortization.totalTenor = number;
                        }

                        if (startDate[runCurrentBlock].ContainsKey(i))
                            setAmortization.startDate = startDate[runCurrentBlock][i];

                        if (endDate[runCurrentBlock].ContainsKey(i))
                            setAmortization.endDate = endDate[runCurrentBlock][i];

                        isExecute = true;

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        if (isExecute == true)
                        {
                            if (setAmortization.acquisition != 0)
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newAmortization.amortizeOneAsset(setAmortization, false);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newAmortization.amortizeOneAsset(setAmortization, false);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                            else
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newAmortization.amortizationProcess(ramStore[currentTable], setAmortization);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newAmortization.amortizationProcess(ramStore[currentTable], setAmortization);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);

                                }
                            }
                        }
                        else
                        {
                            diffTime = DateTime.Now - startTime;
                            printProcessTimeLog(runCurrentBlock, i, diffTime);
                        }


                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void APPENDROW(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        appendRow newAppendRow = new appendRow();
                        appendRowSetting setAppendRow = new appendRowSetting();
                        setAppendRow.appendRow = appendRow[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        foreach (var pair in appendRow[runCurrentBlock][i])
                        {
                            if (!upperColumnName2ID.ContainsKey(pair.Key.ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + pair.Key + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newAppendRow.appendRowProcess(ramStore[currentTable], cellTextStore, cellNumberStore, setAppendRow);
                                OutputCurrentTableMessage(runCurrentBlock, i);

                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newAppendRow.appendRowProcess(ramStore[currentTable], cellTextStore, cellNumberStore, setAppendRow);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);

                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void BUILDBALANCE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        buildBalance newBuildBalance = new buildBalance();
                        buildBalanceSetting setBuildBalance = new buildBalanceSetting();
                        setBuildBalance.columnName = columnName[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "BUILDDAILYBALANCE")
                            setBuildBalance.buildBalanceType = "BUILDDAILYBALANCE";

                        if (currentCommand == "BUILDDAILYBALANCECROSSTABPERIOD")
                            setBuildBalance.buildBalanceType = "BUILDDAILYBALANCECROSSTABPERIOD";

                        if (currentCommand == "BUILDMONTHLYBALANCE")
                            setBuildBalance.buildBalanceType = "BUILDMONTHLYBALANCE";

                        if (currentCommand == "BUILDMONTHLYBALANCECROSSTABPERIOD")
                            setBuildBalance.buildBalanceType = "BUILDMONTHLYBALANCECROSSTABPERIOD";

                        if (currentCommand == "BUILDWEEKLYBALANCE")
                            setBuildBalance.buildBalanceType = "BUILDWEEKLYBALANCE";

                        if (currentCommand == "BUILDWEEKLYBALANCECROSSTABPERIOD")
                            setBuildBalance.buildBalanceType = "BUILDWEEKLYBALANCECROSSTABPERIOD";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newBuildBalance.buildBalanceByPeriod(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setBuildBalance);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newBuildBalance.buildBalanceByPeriod(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setBuildBalance);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void COMPUTECELL(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        if (isExecute == true)
                        {
                            computeCell newComputeCell = new computeCell();
                            computeCellSetting setComputeCell = new computeCellSetting();
                            setComputeCell.calc = aggregateFunction[runCurrentBlock][i][0];
                            setComputeCell.cellName = columnName[runCurrentBlock][i];
                            setComputeCell.decimalPlace = decimalPlace[runCurrentBlock][i][0];

                            currentCell = aggregateByColumnName[runCurrentBlock][i][0];

                            Dictionary<string, string> upperCellName = new Dictionary<string, string>();

                            foreach (var pair in cellTextStore)
                                if (!upperCellName.ContainsKey(pair.Key.ToUpper()))
                                    upperCellName.Add(pair.Key.ToUpper().Trim(), pair.Value);

                            foreach (var pair in cellNumberStore)
                                if (!upperCellName.ContainsKey(pair.Key.ToUpper()))
                                    upperCellName.Add(pair.Key.ToUpper().Trim(), pair.Value.ToString());

                            for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                            {
                                if (!upperCellName.ContainsKey(columnName[runCurrentBlock][i][x].ToString().ToUpper().Trim()))
                                {
                                    if (calcNumberDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                                    {
                                        if (columnName[runCurrentBlock][i][x].Substring(0, 1) != "\"")
                                        {
                                            message = Environment.NewLine + "       " + "Cell name " + "\"" + columnName[runCurrentBlock][i][x] + "\"" + " does not exist in memory.";
                                            processMessage.Enqueue(message);
                                            isExecute = false;
                                            break;
                                        }
                                    }

                                    if (calcTextDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                                    {
                                        if (columnName[runCurrentBlock][i][x].Substring(0, 1) != "\"")
                                        {
                                            message = Environment.NewLine + "       " + "Cell name " + "\"" + columnName[runCurrentBlock][i][x] + "\"" + " does not exist in memory.";
                                            processMessage.Enqueue(message);
                                            isExecute = false;
                                            break;
                                        }
                                    }
                                }
                            }

                            if (isExecute == true)
                            {
                                if (aggregateFunction[runCurrentBlock][i][0].ToUpper() == "COMBINETEXT")
                                {
                                    var currentText = newComputeCell.combineText(cellTextStore, cellNumberStore, setComputeCell);

                                    if (cellTextStore.ContainsKey(currentCell))
                                        cellTextStore.Remove(currentCell);

                                    cellTextStore.Add(currentCell, currentText.ToString());
                                    currentCellValue = cellTextStore[currentCell];
                                    OutputResultCellMessage(runCurrentBlock, i);

                                }
                                else
                                {
                                    var currentNumber = newComputeCell.computeNumber(cellTextStore, cellNumberStore, setComputeCell);

                                    if (cellNumberStore.ContainsKey(currentCell))
                                        cellNumberStore.Remove(currentCell);

                                    cellNumberStore.Add(currentCell, currentNumber);
                                    currentCellValue = cellNumberStore[currentCell].ToString();
                                    OutputResultCellMessage(runCurrentBlock, i);
                                }
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void COMPUTECOLUMN(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                if (calcNumberDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                                {
                                    if (columnName[runCurrentBlock][i][x].Substring(0, 1) != "\"")
                                    {
                                        message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                        processMessage.Enqueue(message);
                                        isExecute = false;
                                        break;
                                    }
                                }
                                if (calcTextDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                                {
                                    if (columnName[runCurrentBlock][i][x].Substring(0, 1) != "\"")
                                    {
                                        message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                        processMessage.Enqueue(message);
                                        isExecute = false;
                                        break;
                                    }
                                }
                            }
                        }

                        if (isExecute == true)
                        {
                            if (calcNumberDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                            {
                                computeColumn newColumn = new computeColumn();
                                computeColumnSetting setComputeColumn = new computeColumnSetting();
                                setComputeColumn.calc = aggregateFunction[runCurrentBlock][i][0];
                                setComputeColumn.refColumnName = columnName[runCurrentBlock][i];
                                setComputeColumn.resultColumnName = aggregateByColumnName[runCurrentBlock][i][0];
                                setComputeColumn.decimalPlace = decimalPlace[runCurrentBlock][i][0];

                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newColumn.calc(ramStore[currentTable], setComputeColumn);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newColumn.calc(ramStore[currentTable], setComputeColumn);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }

                            if (calcTextDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                            {
                                computeTextColumn newTextColumn = new computeTextColumn();
                                computeTextColumnSetting setComputeTextColumn = new computeTextColumnSetting();
                                setComputeTextColumn.calc = aggregateFunction[runCurrentBlock][i][0];
                                setComputeTextColumn.refColumnName = columnName[runCurrentBlock][i];
                                setComputeTextColumn.resultColumnName = aggregateByColumnName[runCurrentBlock][i][0];

                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newTextColumn.calc(ramStore[currentTable], setComputeTextColumn);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newTextColumn.calc(ramStore[currentTable], setComputeTextColumn);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }

                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }                

                    void COPYTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        copyTable newcopyTable = new copyTable();
                        copyTableSetting setCopyTable = new copyTableSetting();
                        setCopyTable.sourceTable = sourceTable[runCurrentBlock][i];
                        setCopyTable.resultTable = resultTable[runCurrentBlock][i];
                        ramStore[resultTable[runCurrentBlock][i]] = newcopyTable.copyTableProcess(ramStore[currentTable]);
                        currentTable = resultTable[runCurrentBlock][i];
                        OutputResultTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void COPYTABLECOMMONTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        copyTable newcopyTable = new copyTable();
                        copyTableSetting setCopyTable2 = new copyTableSetting();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        setCopyTable2.sourceTable = currentTable;

                        setCopyTable2.resultTable = resultTable[runCurrentBlock][i];
                        setCopyTable2.commonTable = masterTable[runCurrentBlock][i];
                        ramStore[resultTable[runCurrentBlock][i]] = newcopyTable.copyTableCommonTableProcess(ramStore, setCopyTable2);
                        currentTable = resultTable[runCurrentBlock][i];
                        OutputResultTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CROSSTAB(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        crosstab newCrosstab = new crosstab();
                        crosstabSetting setCrosstab = new crosstabSetting();
                        setCrosstab.xColumnName = xColumnName[runCurrentBlock][i];
                        setCrosstab.yColumnName = yColumnName[runCurrentBlock][i];
                        setCrosstab.crosstabAggregateFunction = crosstabAggregateFunction[runCurrentBlock][i];
                        setCrosstab.crosstabAggregateByColumnName = crosstabAggregateByColumnName[runCurrentBlock][i];
                        setCrosstab.command = ruleType[runCurrentBlock][i].ToUpper().Trim();                                                                      

                        if (setCrosstab.command != "CROSSTABSQLTABLE")
                        {
                            if (sourceTable[runCurrentBlock].ContainsKey(i))
                                currentTable = sourceTable[runCurrentBlock][i];

                            upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                            for (int x = 0; x < xColumnName[runCurrentBlock][i].Count; x++)
                            {
                                if (!upperColumnName2ID.ContainsKey(xColumnName[runCurrentBlock][i][x].ToUpper()))
                                {
                                    message = Environment.NewLine + "       \"" + xColumnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                    processMessage.Enqueue(message);
                                    isExecute = false;
                                    break;
                                }
                            }

                            for (int x = 0; x < yColumnName[runCurrentBlock][i].Count; x++)
                            {
                                if (!upperColumnName2ID.ContainsKey(yColumnName[runCurrentBlock][i][x].ToUpper()))
                                {
                                    message = Environment.NewLine + "       \"" + yColumnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                    processMessage.Enqueue(message);
                                    isExecute = false;
                                    break;
                                }
                            }


                            for (int x = 0; x < crosstabAggregateByColumnName[runCurrentBlock][i].Count; x++)
                            {
                                if (!upperColumnName2ID.ContainsKey(crosstabAggregateByColumnName[runCurrentBlock][i][x].ToUpper()))
                                {
                                    if (crosstabAggregateByColumnName[runCurrentBlock][i][x].ToUpper() != "NULL")
                                    {
                                        message = Environment.NewLine + "       \"" + crosstabAggregateByColumnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                        processMessage.Enqueue(message);
                                        isExecute = false;
                                        break;
                                    }
                                }
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                if (setCrosstab.command != "CROSSTABSQLTABLE")
                                {
                                    ramStore[currentTable] = newCrosstab.crosstabTable(ramStore[currentTable], setCrosstab);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                            }
                            else
                            {
                                if (setCrosstab.command != "CROSSTABSQLTABLE")
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newCrosstab.crosstabTable(ramStore[currentTable], setCrosstab);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }

                                else if (setCrosstab.command == "CROSSTABSQLTABLE")
                                {
                                    List<string> selectedColumnName = new List<string>();

                                    for (int x = 0; x < xColumnName[runCurrentBlock][i].Count; x++)
                                        selectedColumnName.Add(xColumnName[runCurrentBlock][i][x]);

                                    for (int x = 0; x < yColumnName[runCurrentBlock][i].Count; x++)
                                        selectedColumnName.Add(yColumnName[runCurrentBlock][i][x]);

                                    setCrosstab.currentSQLServer = currentSQLServer;
                                    setCrosstab.currentConnectionString = currentConnectionString;
                                    setCrosstab.resultTable = resultTable[runCurrentBlock][i];
                                    setCrosstab.selectedColumnName = selectedColumnName;

                                    if (sourceTable[runCurrentBlock].ContainsKey(i))
                                        setCrosstab.sourceTable = sourceTable[runCurrentBlock][i];

                                    ramStore[resultTable[runCurrentBlock][i]] = newCrosstab.crosstabTable(null, setCrosstab);
                                    currentTable = resultTable[runCurrentBlock][i];

                                    //  if (classMessage.Contains("Fail"))
                                    //    isExecute = false;

                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CSV2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
                        setCSV2LedgerRAM.filePath = sourceTable[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();                        

                        if (currentCommand.ToUpper() == "ONECOLUMN2LEDGERRAM" || currentCommand.ToUpper() == "TEXT2LEDGERRAM")
                            setCSV2LedgerRAM.isOneColumn = true;
                        else
                            setCSV2LedgerRAM.isOneColumn = false;

                        ramStore[resultTable[runCurrentBlock][i]] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                        currentTable = resultTable[runCurrentBlock][i];
                        OutputResultTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CSV2LEDGERRAMCOMMONTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
                        setCSV2LedgerRAM.commonTable = masterTable[runCurrentBlock][i];
                        setCSV2LedgerRAM.filePath = columnName[runCurrentBlock][i][0];
                        ramStore[resultTable[runCurrentBlock][i]] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                        currentTable = resultTable[runCurrentBlock][i];
                        OutputResultTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CONDITION(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        condition newCondition = new condition();
                        conditionSetting setCondition = new conditionSetting();
                        setCondition.conditionType = conditionType[runCurrentBlock][i];
                        setCondition.selectedCellName = selectedCellName[runCurrentBlock][i];
                        setCondition.compareOperator = compareOperator[runCurrentBlock][i];
                        setCondition.selectedTextNumber = selectedTextNumber[runCurrentBlock][i];

                        if (selectedText[runCurrentBlock].Count > 0)
                            setCondition.selectedText = selectedText[runCurrentBlock][i];

                        if (selectedNumber[runCurrentBlock].Count > 0)
                            setCondition.selectedNumber = selectedNumber[runCurrentBlock][i];

                        if (isExecute == true)
                        {
                            currentCell = resultCell[runCurrentBlock][i];

                            var currentText = newCondition.conditionProcess(cellTextStore, cellNumberStore, setCondition);

                            if (cellTextStore.ContainsKey(currentCell))
                                cellTextStore.Remove(currentCell);

                            cellTextStore.Add(currentCell, currentText.ToString());
                            currentCellValue = cellTextStore[currentCell];
                            OutputResultCellMessage(runCurrentBlock, i);
                        }
                        else
                        {
                            diffTime = DateTime.Now - startTime;
                            printProcessTimeLog(runCurrentBlock, i, diffTime);
                        }
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                        if (ruleType[runCurrentBlock][i].ToUpper().Trim().Contains("ACTION"))
                        {
                            if (cellTextStore[currentCell] == "Y")
                            {
                                if (sourceBlock[runCurrentBlock][i].ToUpper() != "EXIT")
                                    runBlock(sourceBlock[runCurrentBlock][i]);
                                else
                                    isExit[0] = true;
                            }
                        }
                    }

                    void CONTINUEPROCESS(string runCurrentBlock, int i)
                    {
                        runBlock(columnName[runCurrentBlock][i][0]);
                    }

                    void CURRENTTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        currentTable = columnName[runCurrentBlock][i][0];
                        OutputCurrentTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void CURRENTBUILDBALANCESETTING(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        currentBuildBalanceSetting = columnName[runCurrentBlock][i][0];
                        OutputCurrentTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void DATATABLE2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
                        LedgerRAM dataTable2LedgerRAM = currentProcess.dataTable2LedgerRAM(_dataTable, setDataTable2LedgerRAM);
                        currentTable = resultTable[runCurrentBlock][i];
                        ramStore.Add(resultTable[runCurrentBlock][i], dataTable2LedgerRAM);
                        OutputResultTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void DATE2EFFECTIVEDATE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        date2EffectiveDate newDate2EffectiveDate = new date2EffectiveDate();
                        date2EffectiveDateSetting setDate2EffectiveDate = new date2EffectiveDateSetting();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        setDate2EffectiveDate.dateColumnName = columnName[runCurrentBlock][i];
                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newDate2EffectiveDate.date2EffectiveDateProcess(ramStore[currentTable], setDate2EffectiveDate);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newDate2EffectiveDate.date2EffectiveDateProcess(ramStore[currentTable], setDate2EffectiveDate);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void DATE2PERIOD(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        period newPeriod = new period();
                        periodSetting setPeriod = new periodSetting();

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "DATE2MONTHLYPERIOD")
                            setPeriod.periodType = "Month";

                        else if (currentCommand == "DATE2DAILYPERIOD")
                            setPeriod.periodType = "Day";

                        else if (currentCommand == "DATE2WEEKLYPERIOD")
                            setPeriod.periodType = "Week";

                        setPeriod.periodDateColumn = periodDateColumn[runCurrentBlock][i];

                        if (!periodStartDayNumber[runCurrentBlock].ContainsKey(i))
                            setPeriod.periodStartDayNumber = 1;
                        else
                            setPeriod.periodStartDayNumber = periodStartDayNumber[runCurrentBlock][i];

                        if (!periodStartMonthNumber[runCurrentBlock].ContainsKey(i))
                            setPeriod.periodStartMonthNumber = 1;
                        else 
                            setPeriod.periodStartMonthNumber = periodStartMonthNumber[runCurrentBlock][i];

                        if (!periodStartWeekNumber[runCurrentBlock].ContainsKey(i))
                            setPeriod.periodStartWeekNumber = 1;
                        else
                            setPeriod.periodStartWeekNumber = periodStartWeekNumber[runCurrentBlock][i];

                        /*
                        if (!periodType[runCurrentBlock].ContainsKey(i))
                            setPeriod.periodType = "Month";
                        else
                            setPeriod.periodType = periodType[runCurrentBlock][i];
                        */

                        if (!cultureOption[runCurrentBlock].ContainsKey(i))
                            setPeriod.cultureOption = "zh-HK";
                        else
                            setPeriod.cultureOption = cultureOption[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        if (!upperColumnName2ID.ContainsKey(periodDateColumn[runCurrentBlock][i].ToUpper()))
                        {
                            message = Environment.NewLine + "       \"" + periodDateColumn[runCurrentBlock][i] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                            processMessage.Enqueue(message);
                            isExecute = false;
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newPeriod.periodCalc(ramStore[currentTable], setPeriod);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newPeriod.periodCalc(ramStore[currentTable], setPeriod);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void DC(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        DC newDC = new DC();

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                if (currentCommand == "DC2POSITIVENEGATIVE")
                                {
                                    DCsetting setDC2PositiveNegative = new DCsetting();
                                    setDC2PositiveNegative.DC2PositiveNegative = columnName[runCurrentBlock][i];
                                    ramStore[currentTable] = newDC.DC2Number(ramStore[currentTable], setDC2PositiveNegative);
                                }

                                if (currentCommand == "DC2NEGATIVEPOSITIVE")
                                {
                                    DCsetting setDC2NegativePositive = new DCsetting();
                                    setDC2NegativePositive.DC2NegativePositive = columnName[runCurrentBlock][i];
                                    ramStore[currentTable] = newDC.DC2Number(ramStore[currentTable], setDC2NegativePositive);
                                }

                                if (currentCommand == "POSITIVENEGATIVE2DC")
                                {
                                    DCsetting setPositiveNegative2DC = new DCsetting();
                                    setPositiveNegative2DC.PositiveNegative2DC = columnName[runCurrentBlock][i];
                                    ramStore[currentTable] = newDC.number2DC(ramStore[currentTable], setPositiveNegative2DC);
                                }

                                if (currentCommand == "NEGATIVEPOSITIVE2DC")
                                {
                                    DCsetting setNegativePositive2DC = new DCsetting();
                                    setNegativePositive2DC.NegativePositive2DC = columnName[runCurrentBlock][i];
                                    ramStore[currentTable] = newDC.number2DC(ramStore[currentTable], setNegativePositive2DC);
                                }

                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                if (currentCommand == "DC2POSITIVENEGATIVE")
                                {
                                    DCsetting setDC2PositiveNegative = new DCsetting();
                                    setDC2PositiveNegative.DC2PositiveNegative = columnName[runCurrentBlock][i];
                                    ramStore[resultTable[runCurrentBlock][i]] = newDC.DC2Number(ramStore[currentTable], setDC2PositiveNegative);
                                }

                                if (currentCommand == "DC2NEGATIVEPOSITIVE")
                                {
                                    DCsetting setDC2NegativePositive = new DCsetting();
                                    setDC2NegativePositive.DC2NegativePositive = columnName[runCurrentBlock][i];
                                    ramStore[resultTable[runCurrentBlock][i]] = newDC.DC2Number(ramStore[currentTable], setDC2NegativePositive);
                                }

                                if (currentCommand == "POSITIVENEGATIVE2DC")
                                {
                                    DCsetting setPositiveNegative2DC = new DCsetting();
                                    setPositiveNegative2DC.PositiveNegative2DC = columnName[runCurrentBlock][i];
                                    ramStore[resultTable[runCurrentBlock][i]] = newDC.number2DC(ramStore[currentTable], setPositiveNegative2DC);
                                }

                                if (currentCommand == "NEGATIVEPOSITIVE2DC")
                                {
                                    DCsetting setNegativePositive2DC = new DCsetting();
                                    setNegativePositive2DC.NegativePositive2DC = columnName[runCurrentBlock][i];
                                    ramStore[resultTable[runCurrentBlock][i]] = newDC.number2DC(ramStore[currentTable], setNegativePositive2DC);
                                }

                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void DISTINCT(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        distinct newDistinct = new distinct();
                        dinstinctSetting setDistinct = new dinstinctSetting();
                        setDistinct.distinctColumnName = columnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newDistinct.distinctList(ramStore[currentTable], setDistinct);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newDistinct.distinctList(ramStore[currentTable], setDistinct);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void DISABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void ENABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void ENDPROCESS(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);                      
                        message = string.Format(Environment.NewLine);
                        processMessage.Enqueue(message);                       
                        ramStore = null;
                        ruleBytestream = null;                       
                        isProcessEnd = true;
                    }

                    void FILELIST2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        fileList2LedgerRAM newFileList2LedgerRAM = new fileList2LedgerRAM();
                        fileList2LedgerRAMsetting setFileList2LedgerRAM = new fileList2LedgerRAMsetting();
                        setFileList2LedgerRAM.folderPath = folderPath[runCurrentBlock][i];
                        setFileList2LedgerRAM.fileFilter = fileFilter[runCurrentBlock][i];
                        setFileList2LedgerRAM.subDirectory = subDirectory[runCurrentBlock][i];

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newFileList2LedgerRAM.fileList2LedgerRAMProcess(setFileList2LedgerRAM);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newFileList2LedgerRAM.fileList2LedgerRAMProcess(setFileList2LedgerRAM);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void FILTER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        filter newFilter = new filter();
                        filterSetting setFilter = new filterSetting();
                        setFilter.filterType = filterType[runCurrentBlock][i];
                        setFilter.selectedColumnName = columnName[runCurrentBlock][i];
                        setFilter.compareOperator = compareOperator[runCurrentBlock][i];
                        setFilter.selectedTextNumber = selectedTextNumber[runCurrentBlock][i];

                        if (selectedText[runCurrentBlock].Count > 0)
                            setFilter.selectedText = selectedText[runCurrentBlock][i];

                        if (selectedNumber[runCurrentBlock].Count > 0)
                            setFilter.selectedNumber = selectedNumber[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newFilter.filterByOneRow(ramStore[currentTable], setFilter, null);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newFilter.filterByOneRow(ramStore[currentTable], setFilter, null);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void FILTERDISTINCTLIST(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        filter newFilterByDisintctList = new filter();
                        filterSetting setFilterByDistinctList = new filterSetting();

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "ANDFILTER.DISTINCTLIST")
                            setFilterByDistinctList.filterType = "And";

                        if (currentCommand == "ORFILTER.DISTINCTLIST")
                            setFilterByDistinctList.filterType = "Or";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newFilterByDisintctList.filterByDistinctList(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setFilterByDistinctList);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newFilterByDisintctList.filterByDistinctList(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setFilterByDistinctList);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void FILTERCONDITIONLIST(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        filter newFilterByConditionList = new filter();
                        filterSetting setFilterByConditionList = new filterSetting();
                        setFilterByConditionList.command = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "ANDFILTER.CONDITIONLIST")
                            setFilterByConditionList.filterType = "And";

                        if (currentCommand == "ORFILTER.CONDITIONLIST")
                            setFilterByConditionList.filterType = "Or";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newFilterByConditionList.filterByConditionList(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setFilterByConditionList);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newFilterByConditionList.filterByConditionList(ramStore[currentTable], ramStore[masterTable[runCurrentBlock][i]], setFilterByConditionList);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void GROUPBY(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        groupBy newGroupBy = new groupBy();
                        groupBySetting setGroupBy = new groupBySetting();
                        setGroupBy.groupByColumnName = columnName[runCurrentBlock][i];
                        setGroupBy.aggregateFunction = aggregateFunction[runCurrentBlock][i];
                        setGroupBy.aggregateByColumnName = aggregateByColumnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        for (int x = 0; x < aggregateByColumnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(aggregateByColumnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                if (aggregateByColumnName[runCurrentBlock][i][x].ToUpper() != "NULL")
                                {
                                    message = Environment.NewLine + "       \"" + aggregateByColumnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\".";
                                    processMessage.Enqueue(message);
                                    isExecute = false;
                                    break;
                                }
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newGroupBy.groupByList(ramStore[currentTable], setGroupBy);

                                if (ramStore[currentTable].isPeriodEndExist == false)                               
                                    OutputCurrentTableMessage(runCurrentBlock, i);                               
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newGroupBy.groupByList(ramStore[currentTable], setGroupBy);
                                currentTable = resultTable[runCurrentBlock][i];
                                
                                if (ramStore[currentTable].isPeriodEndExist == false)
                                {                                   
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }

                            if (ramStore[currentTable].isPeriodEndExist == false)
                            {
                                diffTime = DateTime.Now - startTime;
                                printProcessTimeLog(runCurrentBlock, i, diffTime);
                            }
                        }
                       

                        if (ramStore[currentTable].isPeriodEndExist == true)
                        {
                            buildBalance newBuildBalance = new buildBalance();
                            buildBalanceSetting setBuildBalance = new buildBalanceSetting();

                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                if (currentBuildBalanceSetting != null)
                                    ramStore[currentTable] = newBuildBalance.buildBalanceProcess(ramStore[currentTable], ramStore[currentBuildBalanceSetting], setBuildBalance);
                                else
                                    ramStore[currentTable] = newBuildBalance.buildBalanceProcess(ramStore[currentTable], null, setBuildBalance);
                               
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {                                                           
                                if (currentBuildBalanceSetting != null)
                                    ramStore[resultTable[runCurrentBlock][i]] = newBuildBalance.buildBalanceProcess(ramStore[currentTable], ramStore[currentBuildBalanceSetting], setBuildBalance);
                                else
                                    ramStore[resultTable[runCurrentBlock][i]] = newBuildBalance.buildBalanceProcess(ramStore[currentTable], null, setBuildBalance);
                                    
                                OutputResultTableMessage(runCurrentBlock, i);                              
                            }                            

                            diffTime = DateTime.Now - startTime;
                            printProcessTimeLog(runCurrentBlock, i, diffTime);
                        }

                       

                    }

                    void JOINTABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        joinTable newJoinTable = new joinTable();
                        joinTableSetting setJoinTable = new joinTableSetting();
                        groupBy newGroupBy2 = new groupBy();
                        groupBySetting setGroupBy2 = new groupBySetting();
                        setJoinTable.leftTable = leftTable[runCurrentBlock][i];
                        setJoinTable.rightTable = rightTable[runCurrentBlock][i];
                        setJoinTable.leftTableColumn = leftTableColumn[runCurrentBlock][i];
                        setJoinTable.rightTableColumn = rightTableColumn[runCurrentBlock][i];
                        setJoinTable.joinTableType = joinTableType[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[setJoinTable.leftTable]);

                        for (int x = 0; x < leftTableColumn[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(leftTableColumn[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + leftTableColumn[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[setJoinTable.rightTable]);

                        for (int x = 0; x < rightTableColumn[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(rightTableColumn[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + rightTableColumn[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            var leftColumnCount = ramStore[setJoinTable.leftTable].factTable.Count;
                            var rightColumnCount = ramStore[setJoinTable.rightTable].factTable.Count;

                            List<string> _aggregateFunction = new List<string>();
                            List<string> _aggregateByColumnName = new List<string>();
                            
                            _aggregateFunction.Add("Count");
                            _aggregateByColumnName.Add("Null");
                            string _masterTable = null;

                            if (!Directory.Exists("Output" + "\\"))
                                Directory.CreateDirectory("Output" + "\\");

                            if (leftColumnCount >= rightColumnCount)
                            {
                                setGroupBy2.groupByColumnName = rightTableColumn[runCurrentBlock][i];
                                setGroupBy2.aggregateFunction = _aggregateFunction;
                                setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                ramStore["_GroupBy1"] = newGroupBy2.groupByList(ramStore[setJoinTable.rightTable], setGroupBy2);
                                LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                StringBuilder LEDGERRAM2CSV1 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy1"], setLEDGERRAM2CSV);

                                if (ramStore["_GroupBy1"].factTable[0].Count < ramStore[setJoinTable.rightTable].factTable[0].Count)
                                {
                                    setGroupBy2.groupByColumnName = leftTableColumn[runCurrentBlock][i];
                                    setGroupBy2.aggregateFunction = _aggregateFunction;
                                    setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                    ramStore["_GroupBy2"] = newGroupBy2.groupByList(ramStore[setJoinTable.leftTable], setGroupBy2);
                                    setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                    StringBuilder LEDGERRAM2CSV2 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy2"], setLEDGERRAM2CSV);

                                    if (ramStore["_GroupBy2"].factTable[0].Count < ramStore[setJoinTable.leftTable].factTable[0].Count)
                                    {
                                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + "!Error_" + setJoinTable.rightTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV1);
                                            toDisk.Close();
                                        }

                                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + "!Error_" + setJoinTable.leftTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV2);
                                            toDisk.Close();
                                        }
                                       
                                        isExecute = false;
                                    }
                                    else
                                        _masterTable = "LeftTable";
                                }
                                else
                                    _masterTable = "RightTable";
                            }

                            if (rightColumnCount > leftColumnCount)
                            {
                                setGroupBy2.groupByColumnName = leftTableColumn[runCurrentBlock][i];
                                setGroupBy2.aggregateFunction = _aggregateFunction;
                                setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                ramStore["_GroupBy1"] = newGroupBy2.groupByList(ramStore[setJoinTable.leftTable], setGroupBy2);
                                LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                StringBuilder LEDGERRAM2CSV1 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy1"], setLEDGERRAM2CSV);

                                if (ramStore["_GroupBy1"].factTable[0].Count < ramStore[setJoinTable.leftTable].factTable[0].Count)
                                {
                                    setGroupBy2.groupByColumnName = rightTableColumn[runCurrentBlock][i];
                                    setGroupBy2.aggregateFunction = _aggregateFunction;
                                    setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                    ramStore["_GroupBy2"] = newGroupBy2.groupByList(ramStore[setJoinTable.rightTable], setGroupBy2);
                                    setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                    StringBuilder LEDGERRAM2CSV2 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy2"], setLEDGERRAM2CSV);

                                    if (ramStore["_GroupBy2"].factTable[0].Count < ramStore[setJoinTable.rightTable].factTable[0].Count)
                                    {
                                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + "!Error_" + setJoinTable.leftTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV1);
                                            toDisk.Close();
                                        }

                                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + "!Error_" + setJoinTable.rightTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV2);
                                            toDisk.Close();
                                        }
                                        
                                        isExecute = false;
                                    }
                                    else
                                        _masterTable = "RightTable";
                                }
                                else
                                    _masterTable = "LeftTable";
                            }

                            if (_masterTable == "RightTable")
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newJoinTable.joinTableList(ramStore[setJoinTable.leftTable], ramStore[setJoinTable.rightTable], setJoinTable);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newJoinTable.joinTableList(ramStore[setJoinTable.leftTable], ramStore[setJoinTable.rightTable], setJoinTable);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                            else if (_masterTable == "LeftTable")
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newJoinTable.joinTableList(ramStore[setJoinTable.rightTable], ramStore[setJoinTable.leftTable], setJoinTable);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newJoinTable.joinTableList(ramStore[setJoinTable.rightTable], ramStore[setJoinTable.leftTable], setJoinTable);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                            else if (_masterTable != "LeftTable" && _masterTable != "RightTable")
                            {
                                message = Environment.NewLine + "       " + "Both tables contain duplicated records of joined columns." + Environment.NewLine + "       See file " + "!Error_" + setJoinTable.rightTable + ".csv and " + "!Error_" + setJoinTable.leftTable + ".csv" + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }
                        else
                        {
                            diffTime = DateTime.Now - startTime;
                            printProcessTimeLog(runCurrentBlock, i, diffTime);
                            isExecute = false;
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);


                    }

                    void JOINTABLEBYCONDITION(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        conditionalJoin newConditionalJoin = new conditionalJoin();
                        conditionalJoinSetting setConditionalJoin = new conditionalJoinSetting();
                        groupBy newGroupBy2 = new groupBy();
                        groupBySetting setGroupBy2 = new groupBySetting();
                        setConditionalJoin.leftTable = leftTable[runCurrentBlock][i];
                        setConditionalJoin.rightTable = rightTable[runCurrentBlock][i];
                        setConditionalJoin.leftTableColumn = leftTableColumn[runCurrentBlock][i];
                        setConditionalJoin.rightTableColumn = rightTableColumn[runCurrentBlock][i];
                        setConditionalJoin.joinTableType = joinTableType[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[setConditionalJoin.leftTable]);

                        for (int x = 0; x < leftTableColumn[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(leftTableColumn[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + leftTableColumn[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[setConditionalJoin.rightTable]);

                        for (int x = 0; x < rightTableColumn[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(rightTableColumn[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + rightTableColumn[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            var leftColumnCount = ramStore[setConditionalJoin.leftTable].factTable.Count;
                            var rightColumnCount = ramStore[setConditionalJoin.rightTable].factTable.Count;

                            List<string> _aggregateFunction = new List<string>();
                            List<string> _aggregateByColumnName = new List<string>();
                            _aggregateFunction.Add("Count");
                            _aggregateByColumnName.Add("Null");
                            string _masterTable = null;

                            if (leftColumnCount >= rightColumnCount)
                            {
                                setGroupBy2.groupByColumnName = rightTableColumn[runCurrentBlock][i];
                                setGroupBy2.aggregateFunction = _aggregateFunction;
                                setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                ramStore["_GroupBy1"] = newGroupBy2.groupByList(ramStore[setConditionalJoin.rightTable], setGroupBy2);
                                LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                StringBuilder LEDGERRAM2CSV1 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy1"], setLEDGERRAM2CSV);

                                if (ramStore["_GroupBy1"].factTable[0].Count < ramStore[setConditionalJoin.rightTable].factTable[0].Count)
                                {
                                    setGroupBy2.groupByColumnName = leftTableColumn[runCurrentBlock][i];
                                    setGroupBy2.aggregateFunction = _aggregateFunction;
                                    setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                    ramStore["_GroupBy2"] = newGroupBy2.groupByList(ramStore[setConditionalJoin.leftTable], setGroupBy2);
                                    setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                    StringBuilder LEDGERRAM2CSV2 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy2"], setLEDGERRAM2CSV);

                                    if (ramStore["_GroupBy2"].factTable[0].Count < ramStore[setConditionalJoin.leftTable].factTable[0].Count)
                                    {
                                        using (StreamWriter toDisk = new StreamWriter("!Error_" + setConditionalJoin.rightTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV1);
                                            toDisk.Close();
                                        }

                                        using (StreamWriter toDisk = new StreamWriter("!Error_" + setConditionalJoin.leftTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV2);
                                            toDisk.Close();
                                        }                                      
                                    }
                                    else
                                        _masterTable = "LeftTable";
                                }
                                else
                                    _masterTable = "RightTable";
                            }

                            if (rightColumnCount > leftColumnCount)
                            {
                                setGroupBy2.groupByColumnName = leftTableColumn[runCurrentBlock][i];
                                setGroupBy2.aggregateFunction = _aggregateFunction;
                                setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                ramStore["_GroupBy1"] = newGroupBy2.groupByList(ramStore[setConditionalJoin.leftTable], setGroupBy2);
                                LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                StringBuilder LEDGERRAM2CSV1 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy1"], setLEDGERRAM2CSV);

                                if (ramStore["_GroupBy1"].factTable[0].Count < ramStore[setConditionalJoin.leftTable].factTable[0].Count)
                                {
                                    setGroupBy2.groupByColumnName = rightTableColumn[runCurrentBlock][i];
                                    setGroupBy2.aggregateFunction = _aggregateFunction;
                                    setGroupBy2.aggregateByColumnName = _aggregateByColumnName;
                                    ramStore["_GroupBy2"] = newGroupBy2.groupByList(ramStore[setConditionalJoin.rightTable], setGroupBy2);
                                    setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                                    StringBuilder LEDGERRAM2CSV2 = currentProcess.LedgerRAM2CSV(ramStore["_GroupBy2"], setLEDGERRAM2CSV);

                                    if (ramStore["_GroupBy2"].factTable[0].Count < ramStore[setConditionalJoin.rightTable].factTable[0].Count)
                                    {
                                        using (StreamWriter toDisk = new StreamWriter("!Error_" + setConditionalJoin.leftTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV1);
                                            toDisk.Close();
                                        }

                                        using (StreamWriter toDisk = new StreamWriter("!Error_" + setConditionalJoin.rightTable + ".csv"))
                                        {
                                            toDisk.Write(LEDGERRAM2CSV2);
                                            toDisk.Close();
                                        }                                       
                                    }
                                    else
                                        _masterTable = "RightTable";
                                }
                                else
                                    _masterTable = "LeftTable";
                            }

                            if (_masterTable == "RightTable")
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newConditionalJoin.conditionalJoinProcess(ramStore[setConditionalJoin.leftTable], ramStore[setConditionalJoin.rightTable], setConditionalJoin);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newConditionalJoin.conditionalJoinProcess(ramStore[setConditionalJoin.leftTable], ramStore[setConditionalJoin.rightTable], setConditionalJoin);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                            else if (_masterTable == "LeftTable")
                            {
                                if (!resultTable[runCurrentBlock].ContainsKey(i))
                                {
                                    ramStore[currentTable] = newConditionalJoin.conditionalJoinProcess(ramStore[setConditionalJoin.rightTable], ramStore[setConditionalJoin.leftTable], setConditionalJoin);
                                    OutputCurrentTableMessage(runCurrentBlock, i);
                                }
                                else
                                {
                                    ramStore[resultTable[runCurrentBlock][i]] = newConditionalJoin.conditionalJoinProcess(ramStore[setConditionalJoin.rightTable], ramStore[setConditionalJoin.leftTable], setConditionalJoin);
                                    currentTable = resultTable[runCurrentBlock][i];
                                    OutputResultTableMessage(runCurrentBlock, i);
                                }
                            }
                            else if (_masterTable != "LeftTable" && _masterTable != "RightTable")
                            {
                                message = Environment.NewLine + "       " + "Both tables contain duplicated records of joined columns." + Environment.NewLine + "       See file " + "!Error_" + setConditionalJoin.rightTable + ".csv and " + "!Error_" + setConditionalJoin.leftTable + ".csv" + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void LEDGERRAM2CSV(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        string _currentTable = null;

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()) && columnName[runCurrentBlock][i][0] != "*")
                            {
                                message = Environment.NewLine + "       \"" + "Error: " + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            LedgerRAM2CSVsetting setLEDGERRAM2CSV = new LedgerRAM2CSVsetting();
                            setLEDGERRAM2CSV.columnName = columnName[runCurrentBlock][i];

                            List<string> _columnName = new List<string>();

                            if (ramStore[currentTable].crosstabHeader == null)
                            {
                                if (setLEDGERRAM2CSV.columnName != null)
                                {
                                    if (setLEDGERRAM2CSV.columnName[0] == "*")
                                    {
                                        for (int x = 0; x < ramStore[currentTable].columnName.Count; x++)
                                            _columnName.Add(ramStore[currentTable].columnName[x]);

                                        setLEDGERRAM2CSV.columnName = _columnName;
                                    }

                                    selectColumn newSelectColumn = new selectColumn();
                                    selectColumnSetting setSelectColumn = new selectColumnSetting();
                                    setSelectColumn.selectColumn = setLEDGERRAM2CSV.columnName;
                                    setSelectColumn.selectType = "Add";                                   
                                  
                                    ramStore[currentTable] = newSelectColumn.selectColumnName(ramStore[currentTable], setSelectColumn);                                  
                                }
                            }

                            _currentTable = currentTable;
                            StringBuilder _LEDGERRAM2CSV = currentProcess.LedgerRAM2CSV(ramStore[currentTable], setLEDGERRAM2CSV);
                            OutputNonLedgerRAMMessage(runCurrentBlock, i, "CSV", _currentTable);                            

                            if (!Directory.Exists("Output" + "\\"))
                                Directory.CreateDirectory("Output" + "\\");

                            using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + resultTable[runCurrentBlock][i]))
                            {
                                toDisk.Write(_LEDGERRAM2CSV);
                                toDisk.Close();
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void LEDGERRAM2DATATABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        LedgerRAM2DataTablesetting setLedgerRAM2DataTable = new LedgerRAM2DataTablesetting();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        _dataTable = currentProcess.LedgerRAM2DataTable(ramStore[currentTable], setLedgerRAM2DataTable);

                        OutputDataTableMessage(runCurrentBlock, i);
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void LEDGERRAM2HTML(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        LedgerRAM2HTMLsetting setLEDGERRAM2HTML = new LedgerRAM2HTMLsetting();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        StringBuilder _LEDGERRAM2HTML = currentProcess.LedgerRAM2HTML(ramStore[currentTable], setLEDGERRAM2HTML);
                        OutputNonLedgerRAMMessage(runCurrentBlock, i, "HTML", currentTable);

                        if (!Directory.Exists("Output" + "\\"))
                            Directory.CreateDirectory("Output" + "\\");

                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + resultTable[runCurrentBlock][i]))
                        {
                            toDisk.Write(_LEDGERRAM2HTML);
                            toDisk.Close();
                        }
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void LEDGERRAM2JSON(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        LedgerRAM2JSONsetting setLEDGERRAM2JSON = new LedgerRAM2JSONsetting();
                        setLEDGERRAM2JSON.tableName = resultTable[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        StringBuilder _LEDGERRAM2JSON = currentProcess.LedgerRAM2JSON(ramStore[currentTable], setLEDGERRAM2JSON);
                        OutputNonLedgerRAMMessage(runCurrentBlock, i, "JSON", currentTable);

                        if (!Directory.Exists("Output" + "\\"))
                            Directory.CreateDirectory("Output" + "\\");

                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + resultTable[runCurrentBlock][i]))
                        {
                            toDisk.Write(_LEDGERRAM2JSON);
                            toDisk.Close();
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void LEDGERRAM2XML(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        LedgerRAM2XMLsetting setLEDGERRAM2XML = new LedgerRAM2XMLsetting();
                        setLEDGERRAM2XML.tableName = resultTable[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        StringBuilder _LEDGERRAM2XML = currentProcess.LedgerRAM2XML(ramStore[currentTable], setLEDGERRAM2XML);
                        OutputNonLedgerRAMMessage(runCurrentBlock, i, "XML", currentTable);

                        if (!Directory.Exists("Output" + "\\"))
                            Directory.CreateDirectory("Output" + "\\");

                        using (StreamWriter toDisk = new StreamWriter("Output" + "\\" + resultTable[runCurrentBlock][i]))
                        {
                            toDisk.Write(_LEDGERRAM2XML);
                            toDisk.Close();
                        }
                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void MANYCSV2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        manyCSV2LedgerRAM newManyCSV2LedgerRAM = new manyCSV2LedgerRAM();
                        manyCSV2LedgerRAMsetting setManyCSV2LedgerRAM = new manyCSV2LedgerRAMsetting();
                        setManyCSV2LedgerRAM.folderPath = folderPath[runCurrentBlock][i];
                        setManyCSV2LedgerRAM.fileFilter = fileFilter[runCurrentBlock][i];
                        setManyCSV2LedgerRAM.subDirectory = subDirectory[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "REVERSEMANYCROSSTABCSV2LEDGERRAM")
                            setManyCSV2LedgerRAM.tableType = "Crosstab";
                        else
                            setManyCSV2LedgerRAM.tableType = "TransactionList";

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newManyCSV2LedgerRAM.manyCSV2LedgerRAMProcess(ramStore, setManyCSV2LedgerRAM);
                                
                                if (ramStore[currentTable] == null)
                                    ramStore[currentTable] = newManyCSV2LedgerRAM.manyDifferentCSV2LedgerRAMProcess(ramStore, setManyCSV2LedgerRAM);

                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newManyCSV2LedgerRAM.manyCSV2LedgerRAMProcess(ramStore, setManyCSV2LedgerRAM);

                                if (ramStore[resultTable[runCurrentBlock][i]] == null)
                                    ramStore[resultTable[runCurrentBlock][i]] = newManyCSV2LedgerRAM.manyDifferentCSV2LedgerRAMProcess(ramStore, setManyCSV2LedgerRAM);

                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                            diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void MERGETABLE(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        mergeTable newMergeTable = new mergeTable();
                        mergeTableSetting setMergeTable = new mergeTableSetting();
                        setMergeTable.tableName = columnName[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!ramStore.ContainsKey(columnName[runCurrentBlock][i][x]))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                if (currentCommand == "MERGETABLE")
                                    ramStore[currentTable] = newMergeTable.mergeTableProcess(ramStore, setMergeTable);

                                else if (currentCommand == "COMBINETABLEBYCOMMONCOLUMN")
                                    ramStore[currentTable] = newMergeTable.combineTableByCommonColumnProcess(ramStore, setMergeTable);

                                else if (currentCommand == "MERGECOMMONTABLE")
                                    ramStore[currentTable] = newMergeTable.mergeCommonTableProcess(ramStore, setMergeTable);

                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                if (currentCommand == "MERGETABLE")
                                    ramStore[resultTable[runCurrentBlock][i]] = newMergeTable.mergeTableProcess(ramStore, setMergeTable);

                                else if (currentCommand == "COMBINETABLEBYCOMMONCOLUMN")
                                    ramStore[resultTable[runCurrentBlock][i]] = newMergeTable.combineTableByCommonColumnProcess(ramStore, setMergeTable);

                                else if (currentCommand == "MERGECOMMONTABLE")
                                    ramStore[resultTable[runCurrentBlock][i]] = newMergeTable.mergeCommonTableProcess(ramStore, setMergeTable);

                                currentTable = resultTable[runCurrentBlock][i];

                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void NUMBER2TEXT(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        number2Text newNumber2Text = new number2Text();
                        number2TextSetting setNumber2Text = new number2TextSetting();

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        setNumber2Text.number2Text = columnName[runCurrentBlock][i];
                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newNumber2Text.number2TextList(ramStore[currentTable], setNumber2Text);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newNumber2Text.number2TextList(ramStore[currentTable], setNumber2Text);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void ORDERBY(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        orderBy newOrderBy = new orderBy();
                        orderBySetting setOrderBy = new orderBySetting();
                        setOrderBy.orderByColumnName = orderByColumnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        foreach (var pair in orderByColumnName[runCurrentBlock][i])
                        {
                            if (!upperColumnName2ID.ContainsKey(pair.Key.ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + pair.Key + "\" is not a valid column of the table " + currentTable + "\"" + "\"."; 
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newOrderBy.orderByList(ramStore[currentTable], setOrderBy);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newOrderBy.orderByList(ramStore[currentTable], setOrderBy);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void PROCESS(string runCurrentBlock, int i)
                    {
                        if (columnName[runCurrentBlock][i][0].ToUpper() != "EXIT")
                        {  
                            var nextBlock = columnName[runCurrentBlock][i][0];

                            for (int r = 0; r < ruleDetail[nextBlock].Count; r++)
                            {
                                if(isExecute == true)
                                    commandDict[ruleType[nextBlock][r].ToUpper().Trim()](nextBlock, r);
                            }
                        }
                    }

                    void PARALLELPROCESS(string runCurrentBlock, int i)
                    {
                        var nextBlock = columnName[runCurrentBlock][i][0];

                        for (int worker = 0; worker < ruleDetail[nextBlock].Count; worker++)
                            writeColumnThread.TryAdd(worker, new ruleProcessing());                                            

                        if (columnName[runCurrentBlock][i][0].ToUpper() != "EXIT")
                        {
                            var options = new ParallelOptions()
                            {
                                MaxDegreeOfParallelism = 20
                            };

                            Parallel.For(0, ruleDetail[nextBlock].Count, options, r =>
                            {                               
                                //ruleProcessing newProcess = new ruleProcessing();
                                parallel newParallel = new parallel();
                                newParallel.runParallelFor(nextBlock, r, isParallelProcess, commandDict, checkThreadCompleted, ruleType);
                            });

                            do
                            {
                                Thread.Sleep(2);
                            } while (checkThreadCompleted.Count < ruleDetail[nextBlock].Count);

                            /*
                            Console.WriteLine();

                            foreach (var pair in tableSizeMessage)
                            {
                                foreach (var pair2 in tableSizeMessage[pair.Key])
                                    Console.WriteLine(pair.Key + "  " + pair2.Key + "  " + pair2.Value);
                            }
                            */
                        }
                    }
                 
                    void REPLACERULE(string runCurrentBlock, int i)
                    {


                    }

                    void REVERSECROSSTAB(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        reverseCrosstab newReverseCrosstab = new reverseCrosstab();
                        reverseCrosstabSetting setReverseCrosstab = new reverseCrosstabSetting();
                        setReverseCrosstab.columnName = columnName[runCurrentBlock][i];                       

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()) && columnName[runCurrentBlock][i][x] != "*")
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine; 
                                processMessage.Enqueue(message);
                                isExecute = false;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newReverseCrosstab.reverseCrosstabProcess(ramStore[currentTable], setReverseCrosstab);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newReverseCrosstab.reverseCrosstabProcess(ramStore[currentTable], setReverseCrosstab);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }
                      

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void REVERSECROSSTABCSV2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
                        setCSV2LedgerRAM.filePath = sourceTable[runCurrentBlock][i];
                        ramStore[resultTable[runCurrentBlock][i]] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                        currentTable = resultTable[runCurrentBlock][i];

                        reverseCrosstab newReverseCrosstab = new reverseCrosstab();
                        reverseCrosstabSetting setReverseCrosstab = new reverseCrosstabSetting();                                          
                       
                        if (!resultTable[runCurrentBlock].ContainsKey(i))
                        {
                            ramStore[currentTable] = newReverseCrosstab.reverseCrosstabProcess(ramStore[currentTable], setReverseCrosstab);
                            OutputCurrentTableMessage(runCurrentBlock, i);
                        }
                        else
                        {
                            ramStore[resultTable[runCurrentBlock][i]] = newReverseCrosstab.reverseCrosstabProcess(ramStore[currentTable], setReverseCrosstab);
                            currentTable = resultTable[runCurrentBlock][i];
                            OutputResultTableMessage(runCurrentBlock, i);
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void REVERSEDC(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        DC newReverseDC = new DC();
                        DCsetting setReverseDC = new DCsetting();
                        setReverseDC.ReverseDC = true;

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newReverseDC.reverseDC(ramStore[currentTable], setReverseDC);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newReverseDC.reverseDC(ramStore[currentTable], setReverseDC);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void REVERSENUMBER(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        reverseNumber newReverseNumber = new reverseNumber();
                        reverseNumberSetting setReverseNumber = new reverseNumberSetting();
                        setReverseNumber.numberTypeColumnName = columnName[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine;
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newReverseNumber.reverseNumberProcess(ramStore[currentTable], setReverseNumber);
                                OutputCurrentTableMessage(runCurrentBlock, i);

                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newReverseNumber.reverseNumberProcess(ramStore[currentTable], setReverseNumber);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }

                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);


                    }

                    void RULE2LEDGERRAM(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        rule2LedgerRAM newRule2LedgerRAM = new rule2LedgerRAM();
                        rule2LedgerRAMsetting setRule2LedgerRAM = new rule2LedgerRAMsetting();
                        setRule2LedgerRAM.filePath = columnName[runCurrentBlock][i][0];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][0].ToUpper()))
                        {
                            message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][0] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine; 
                            processMessage.Enqueue(message); 
                            isExecute = false;
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newRule2LedgerRAM.rule2LedgerRAMprocess(ramStore[currentTable], setRule2LedgerRAM);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newRule2LedgerRAM.rule2LedgerRAMprocess(ramStore[currentTable], setRule2LedgerRAM);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);
                    }

                    void SELECTCOLUMN(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);
                        selectColumn newSelectColumn = new selectColumn();
                        selectColumnSetting setSelectColumn = new selectColumnSetting();
                        setSelectColumn.selectColumn = columnName[runCurrentBlock][i];

                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();

                        if (currentCommand == "SELECTCOLUMN")
                            setSelectColumn.selectType = "Add";
                        else if (currentCommand == "REMOVECOLUMN")
                            setSelectColumn.selectType = "Remove";

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"." + Environment.NewLine + Environment.NewLine; 
                                processMessage.Enqueue(message);
                                isExecute = false;
                                break;
                            }
                        }

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newSelectColumn.selectColumnName(ramStore[currentTable], setSelectColumn);
                                OutputCurrentTableMessage(runCurrentBlock, i);
                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newSelectColumn.selectColumnName(ramStore[currentTable], setSelectColumn);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void TABLE2CELL(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        for (int x = 0; x < columnName[runCurrentBlock][i].Count; x++)
                        {
                            if (!upperColumnName2ID.ContainsKey(columnName[runCurrentBlock][i][x].ToUpper()))
                            {
                                if (calc2CellDict.ContainsKey(aggregateFunction[runCurrentBlock][i][0].ToUpper()))
                                {
                                    if (double.TryParse(columnName[runCurrentBlock][i][x], out double number) != true)
                                    {
                                        if (aggregateFunction[runCurrentBlock][i][0].ToUpper() != "CELLADDRESS")
                                        {
                                            message = Environment.NewLine + "       \"" + columnName[runCurrentBlock][i][x] + "\" is not a valid column of the table " + "\"" + currentTable + "\"."; 
                                            processMessage.Enqueue(message);
                                            isExecute = false;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (isExecute == true)
                        {
                            table2Cell newTable2Cell = new table2Cell();
                            table2CellSetting setTable2Cell = new table2CellSetting();
                            setTable2Cell.calc2Cell = aggregateFunction[runCurrentBlock][i][0];
                            setTable2Cell.refColumnName = columnName[runCurrentBlock][i];
                            setTable2Cell.decimalPlace = decimalPlace[runCurrentBlock][i][0];

                            if (calc2CellDict.ContainsKey(setTable2Cell.calc2Cell.ToUpper()))
                            {
                                currentCell = aggregateByColumnName[runCurrentBlock][i][0];

                                if (aggregateFunction[runCurrentBlock][i][0].ToUpper() == "CELLADDRESS")
                                {
                                    var currentText = newTable2Cell.cellAddress(ramStore[currentTable], setTable2Cell);

                                    if (cellTextStore.ContainsKey(currentCell))
                                        cellTextStore.Remove(currentCell);

                                    cellTextStore.Add(currentCell, currentText.ToString());
                                    currentCellValue = cellTextStore[currentCell];
                                    OutputResultCellMessage(runCurrentBlock, i);

                                }
                                else
                                {
                                    var currentNumber = newTable2Cell.table2CellProcess(ramStore[currentTable], setTable2Cell);

                                    if (cellNumberStore.ContainsKey(currentCell))
                                        cellNumberStore.Remove(currentCell);

                                    cellNumberStore.Add(currentCell, currentNumber);
                                    currentCellValue = cellNumberStore[currentCell].ToString();

                                    OutputResultCellMessage(runCurrentBlock, i);

                                }
                            }

                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);


                    }

                    void VOUCHERENTRY(string runCurrentBlock, int i)
                    {
                        startTime = DateTime.Now;
                        printProcessRuleLog(runCurrentBlock, i);

                        voucherEntry newVoucherEntry = new voucherEntry();
                        voucherEntrySetting setVoucherEntry = new voucherEntrySetting();
                        setVoucherEntry.account = account[runCurrentBlock][i];
                        setVoucherEntry.amount = amount[runCurrentBlock][i];
                        setVoucherEntry.drCr = drCr[runCurrentBlock][i];

                        if (sourceTable[runCurrentBlock].ContainsKey(i))
                            currentTable = sourceTable[runCurrentBlock][i];

                        upperColumnName2ID = currentProcess.convertColumnName2Upper(ramStore[currentTable]);

                        if (isExecute == true)
                        {
                            if (!resultTable[runCurrentBlock].ContainsKey(i))
                            {
                                ramStore[currentTable] = newVoucherEntry.voucherEntryProcess(ramStore[currentTable], setVoucherEntry);
                                OutputCurrentTableMessage(runCurrentBlock, i);

                            }
                            else
                            {
                                ramStore[resultTable[runCurrentBlock][i]] = newVoucherEntry.voucherEntryProcess(ramStore[currentTable], setVoucherEntry);
                                currentTable = resultTable[runCurrentBlock][i];
                                OutputResultTableMessage(runCurrentBlock, i);
                            }
                        }
                        else
                        {
                            diffTime = DateTime.Now - startTime;
                            printProcessTimeLog(runCurrentBlock, i, diffTime);

                        }

                        diffTime = DateTime.Now - startTime;
                        printProcessTimeLog(runCurrentBlock, i, diffTime);

                    }

                    void printProcessRuleLog(string runCurrentBlock, int i)
                    {
                        if (isParallelProcess[runCurrentBlock][i] == false)
                            message = Environment.NewLine + runCurrentBlock + "(" + (i + 1) + ") " + recognizedRule[runCurrentBlock][i].ToString();                       
                        else
                            message = Environment.NewLine + runCurrentBlock + "(" + (i + 1) + ") " + recognizedRule[runCurrentBlock][i].ToString() + Environment.NewLine;

                     //   if (isParallelProcess[runCurrentBlock][i] == true)
                            processMessage.Enqueue(message);                        
                    }
                   
                    void OutputCurrentTableMessage(string runCurrentBlock, int i)
                    {
                        var currentMessage = "" + currentTable + "(Column:" + string.Format("{0:#,0}", ramStore[currentTable].factTable.Count) + ", Row:" + string.Format("{0:#,0}", ramStore[currentTable].factTable[0].Count) + ")";

                        if (!tableSizeMessage[runCurrentBlock].ContainsKey(i))
                            tableSizeMessage[runCurrentBlock].Add(i, currentMessage);
                        else
                            tableSizeMessage[runCurrentBlock][i] = currentMessage;
                    }
                   
                    void OutputResultTableMessage(string runCurrentBlock, int j)
                    {                     
                        var currentMessage = "" + currentTable + "(Column:" + string.Format("{0:#,0}", ramStore[resultTable[runCurrentBlock][j]].factTable.Count) + ", Row:" + string.Format("{0:#,0}", ramStore[resultTable[runCurrentBlock][j]].factTable[0].Count) + ")";

                        if (!tableSizeMessage[runCurrentBlock].ContainsKey(j))
                            tableSizeMessage[runCurrentBlock].Add(j, currentMessage);
                        else
                            tableSizeMessage[runCurrentBlock][j] = currentMessage;                       
                    }                 

                    void OutputResultCellMessage(string runCurrentBlock, int i)
                    {
                        var currentMessage = currentCell + " = " + currentCellValue;

                        if (!tableSizeMessage[runCurrentBlock].ContainsKey(i))
                            tableSizeMessage[runCurrentBlock].Add(i, currentMessage);
                        else
                            tableSizeMessage[runCurrentBlock][i] = currentMessage;
                    }

                    void printProcessTimeLog(string runCurrentBlock, int i, TimeSpan timeDiff)
                    {
                        var currentCommand = ruleType[runCurrentBlock][i].ToUpper().Trim();
                        string message1;
                        string message2 = null;

                        if (isParallelProcess[runCurrentBlock][i] == false)
                            message1 = Environment.NewLine + string.Format("Time:{0:0.000}", timeDiff.TotalSeconds) + "s ";
                        else
                            message1 = Environment.NewLine + runCurrentBlock + "(" + (i+1) + ") Completed by " + string.Format("Time:{0:0.000}", timeDiff.TotalSeconds) + "s ";
                       
                        if (tableSizeMessage[runCurrentBlock].ContainsKey(i))
                            message2 = tableSizeMessage[runCurrentBlock][i];

                        if (isParallelProcess[runCurrentBlock][i] == false)
                        {
                          //  processMessage.Enqueue(message);
                            processMessage.Enqueue(message1 + message2 + Environment.NewLine);
                        }
                        else
                            processMessage.Enqueue(message1 + message2 + Environment.NewLine);
                    }

                    void OutputNonLedgerRAMMessage(string runCurrentBlock, int i, string tablename, string tableName2)
                    {
                        var columnCount = ramStore[tableName2].factTable.Count;
                        var rowCount = ramStore[tableName2].factTable[0].Count;
                        var currentMessage = "" + tablename + "(Column:" + string.Format("{0:#,0}", columnCount) + ", Row:" + string.Format("{0:#,0}", rowCount) + ")";
                        tableSizeMessage[runCurrentBlock].Add(i, currentMessage);
                    }

                    void OutputDataTableMessage(string runCurrentBlock, int i)
                    {
                        var columnCount = _dataTable.Columns.Count;
                        var rowCount = _dataTable.Rows.Count + 1;
                        var currentMessage = "" + "DataTable" + "(Column:" + string.Format("{0:#,0}", columnCount) + ", Row:" + string.Format("{0:#,0}", rowCount) + ")";
                        tableSizeMessage[runCurrentBlock].Add(i, currentMessage);
                    }
                }                
            }

            return isProcessEnd;
        }

        public void runParallelFor(string nextBlock, int r, Dictionary<string, Dictionary<int, bool>> isParallelProcess, Dictionary<string, Action<string, int>> commandDict, ConcurrentQueue<int> checkThreadCompleted, Dictionary<string, Dictionary<int, string>> ruleType)
        {
            isParallelProcess[nextBlock][r] = true;
            commandDict[ruleType[nextBlock][r].ToUpper().Trim()](nextBlock, r);
            checkThreadCompleted.Enqueue(r);
        }
    }
}
