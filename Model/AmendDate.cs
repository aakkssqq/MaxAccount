using System;
using System.Collections.Generic;
using System.Globalization;

namespace MaxAccount
{
    public class amendDateSetting
    {
        public int rowThread = 100;
        public string sourceDataName { get; set; }
        public List<string> columnName { get; set; }
        public int addDay { get; set; }
        public int addMonth { get; set; }
        public int addYear { get; set; }
        public int day { get; set; }
        public int month { get; set; }
        public int year { get; set; }
        public int startMonth { get; set; }
        public int startDay { get; set; }
        public int startWeek { get; set; }
        public int nextPeriodAddDay { get; set; }        
        public string ruleType { get; set; }
        public string periodType { get; set; }
        public string cultureOption { get; set; }
    }

    public class amendDate
    { 
        public LedgerRAM amendDateByTable(LedgerRAM currentTable, amendDateSetting currentSetting)
        {
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();

            List<int> dateColumnID = new List<int>();         

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {               
                if (currentTable.dataType[x] == "Date")
                {
                    dateColumnID.Add(x);                   
                    key2Value.Add(x, new Dictionary<double, string>());
                    value2Key.Add(x, new Dictionary<string, double>());
                }
            }

            for (int x = 0; x < dateColumnID.Count; x++)
            {
                foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                {                   
                    var currentDate = amendDateByCell(pair.Value, currentSetting);                   

                    key2Value[dateColumnID[x]].Add(pair.Key, currentDate);

                    if (!value2Key[dateColumnID[x]].ContainsKey(currentDate))
                        value2Key[dateColumnID[x]].Add(currentDate, pair.Key);                   
                }
            }


            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.dataType[x] != "Number")
                {
                    if (currentTable.dataType[x] == "Date")
                    {                       
                        resultKey2Value.Add(x, key2Value[dateColumnID[x]]);
                        resultValue2Key.Add(x, value2Key[dateColumnID[x]]);
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
        public LedgerRAM amendDateByColumn(LedgerRAM currentTable, LedgerRAM ledgerMaster, amendDateSetting currentSetting)
        {
            List<string> originalColumnOrder = new List<string>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
                originalColumnOrder.Add(currentTable.columnName[x]);

            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();

            List<int> dateColumnID = new List<int>();

            /*
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);
            */

            for (int x = 0; x < currentSetting.columnName.Count; x++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.columnName[x].ToUpper()))
                    dateColumnID.Add(currentTable.upperColumnName2ID[currentSetting.columnName[x].ToUpper()]);
            }
          
            int count = 0;
            for (int x = 0; x < dateColumnID.Count; x++)
            {
                factTable.Add(dateColumnID[x], new List<double>());
                key2Value.Add(dateColumnID[x], new Dictionary<double, string>());
                value2Key.Add(dateColumnID[x], new Dictionary<string, double>());

                factTable[dateColumnID[x]].Add(dateColumnID[x]);

                for (int y = 1; y < currentTable.factTable[dateColumnID[x]].Count; y++)
                {
                    var text = amendDateByCell(currentTable.key2Value[dateColumnID[x]][currentTable.factTable[dateColumnID[x]][y]], currentSetting);

                    if (value2Key[dateColumnID[x]].ContainsKey(text))
                        factTable[dateColumnID[x]].Add(value2Key[dateColumnID[x]][text]);
                    else 
                    {
                        count = value2Key[dateColumnID[x]].Count;
                        key2Value[dateColumnID[x]].Add(count, text);
                        value2Key[dateColumnID[x]].Add(text, count);
                        factTable[dateColumnID[x]].Add(count);
                    }
                }
            }

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.dataType[x] != "Number")
                {
                    if (dateColumnID.Contains(x))
                    {
                        resultFactTable.Add(x, factTable[x]);
                        resultKey2Value.Add(x, key2Value[x]);
                        resultValue2Key.Add(x, value2Key[x]);
                    }
                    else
                    {
                        resultFactTable.Add(x, currentTable.factTable[x]);
                        resultKey2Value.Add(x, currentTable.key2Value[x]);
                        resultValue2Key.Add(x, currentTable.value2Key[x]);
                    }
                }
                else
                    resultFactTable.Add(x, currentTable.factTable[x]);
            }

            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultFactTable;           
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            if (currentSetting.ruleType.ToUpper() == "REVERSEMONTHLYVOUCHER" || currentSetting.ruleType.ToUpper() == "REVERSEDAILYVOUCHER" || currentSetting.ruleType.ToUpper() == "REVERSEWEEKLYVOUCHER")
            {
                string periodTypeColumnName = null;

                List<string> originalColumnName = new List<string>();

                for (int x = 0; x < currentTable.columnName.Count; x++)
                    originalColumnName.Add(currentTable.columnName[x]);

                DC newReverseDC = new DC();
                DCsetting setReverseDC = new DCsetting();
                setReverseDC.ReverseDC = true;

                currentOutput = newReverseDC.reverseDC(currentOutput, setReverseDC);

                if (currentSetting.startMonth != 0 || currentSetting.startWeek != 0 || currentSetting.startDay != 0)
                {
                    period newPeriod = new period();
                    periodSetting setPeriod = new periodSetting();
                    setPeriod.periodDateColumn = currentSetting.columnName[0];

                    if (currentSetting.periodType == "MONTH")                                           
                        setPeriod.periodStartMonthNumber = currentSetting.startMonth;                    

                    if (currentSetting.periodType == "DAY")
                        setPeriod.periodStartDayNumber = currentSetting.startDay;

                    if (currentSetting.periodType == "WEEK")
                    {
                        setPeriod.periodStartWeekNumber = currentSetting.startWeek;
                        setPeriod.cultureOption = currentSetting.cultureOption;
                    }

                    setPeriod.periodType = currentSetting.periodType;                  

                    currentOutput = newPeriod.periodCalc(currentOutput, setPeriod);                   
                }

                List<string> joinColumn = new List<string>();
                List<string> removeColumn = new List<string>();
                List<string> removeColumn2 = new List<string>();
                List<string> yearEndAccountColumn = new List<string>();
                Dictionary<string, string> yearEndColumn2AccountBalanceColumn = new Dictionary<string, string>();

                if (ledgerMaster != null)
                {
                    for (int x = 0; x < ledgerMaster.columnName.Count; x++)
                    {
                        if (currentTable.upperColumnName2ID.ContainsKey(ledgerMaster.columnName[x].ToUpper()))
                            joinColumn.Add(ledgerMaster.columnName[x]);
                        else
                        {
                            if (ledgerMaster.columnName[x].ToUpper().Contains("END:"))
                            {
                                var col = ledgerMaster.columnName[x].Substring(ledgerMaster.columnName[x].IndexOf(":") + 1, ledgerMaster.columnName[x].Length - ledgerMaster.columnName[x].IndexOf(":") - 1);
                                removeColumn2.Add(col);
                                yearEndColumn2AccountBalanceColumn.Add(ledgerMaster.columnName[x], col);
                            }
                            else
                            {
                                removeColumn.Add(ledgerMaster.columnName[x]);                             
                            }
                        }
                    }                    
                    
                    selectColumn newSelectColumn = new selectColumn();
                    selectColumnSetting setSelectColumn = new selectColumnSetting();
                    setSelectColumn.selectColumn = removeColumn;
                    setSelectColumn.selectType = "Remove";

                    ledgerMaster = newSelectColumn.selectColumnName(ledgerMaster, setSelectColumn);

                    conditionalJoin newConditionalJoin = new conditionalJoin();
                    conditionalJoinSetting setConditionalJoin = new conditionalJoinSetting();                  
                  
                    setConditionalJoin.leftTableColumn = joinColumn;
                    setConditionalJoin.rightTableColumn = joinColumn;
                    setConditionalJoin.joinTableType = "ConditionalJoin";

                    currentOutput = newConditionalJoin.conditionalJoinProcess(currentOutput, ledgerMaster, setConditionalJoin);

                    // upperColumnName2ID.Clear();

                    /*
                    Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();

                    foreach (var pair in currentOutput.columnName)
                        resultUpperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);
                    */

                    List<int> sourceAccountBalanceColumnID = new List<int>();
                    List<int> resultAccountBalanceColumnID = new List<int>();

                    foreach (var pair in yearEndColumn2AccountBalanceColumn)
                    {                      
                        sourceAccountBalanceColumnID.Add(currentOutput.upperColumnName2ID[pair.Value.ToUpper()]);
                        resultAccountBalanceColumnID.Add(currentOutput.upperColumnName2ID[pair.Key.ToUpper()]);
                    }

                    Dictionary<int, List<double>> revisedBalanceAccountfactTable = new Dictionary<int, List<double>>();
                    Dictionary<int, Dictionary<double, string>> revisedBalanceAccountKey2Value = new Dictionary<int, Dictionary<double, string>>();
                    Dictionary<int, Dictionary<string, double>> revisedBalanceAccountValue2Key = new Dictionary<int, Dictionary<string, double>>();

                    for (int i = 0; i < sourceAccountBalanceColumnID.Count; i++)
                    {
                        revisedBalanceAccountfactTable.Add(sourceAccountBalanceColumnID[i], new List<double>());
                        revisedBalanceAccountKey2Value.Add(sourceAccountBalanceColumnID[i], new Dictionary<double, string>());
                        revisedBalanceAccountValue2Key.Add(sourceAccountBalanceColumnID[i], new Dictionary<string, double>());
                        revisedBalanceAccountfactTable[sourceAccountBalanceColumnID[i]].Add(sourceAccountBalanceColumnID[i]);

                        /*
                        foreach (var pair in currentOutput.key2Value[sourceAccountBalanceColumnID[i]])
                        {
                            revisedBalanceAccountKey2Value[sourceAccountBalanceColumnID[i]].Add(pair.Key, pair.Value);
                            revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].Add(pair.Value, pair.Key);
                        }
                        */

                        if (currentSetting.periodType == "MONTH")
                            periodTypeColumnName = "PERIOD CHANGE";

                        else if (currentSetting.periodType == "DAY")
                            periodTypeColumnName = "DPERIOD CHANGE";

                        else if (currentSetting.periodType == "WEEK")
                            periodTypeColumnName = "WPERIOD CHANGE";

                        for (int y = 1; y < currentOutput.factTable[currentOutput.upperColumnName2ID[periodTypeColumnName]].Count; y++)
                        {
                            var period = currentOutput.key2Value[currentOutput.upperColumnName2ID[periodTypeColumnName]][currentOutput.factTable[currentOutput.upperColumnName2ID[periodTypeColumnName]][y]];                              

                            if (!period.Contains("p01") && !period.Contains("p001"))
                            {
                                var text = currentOutput.key2Value[sourceAccountBalanceColumnID[i]][currentOutput.factTable[sourceAccountBalanceColumnID[i]][y]];                                  

                                if (revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].ContainsKey(text)) // same master record
                                    revisedBalanceAccountfactTable[sourceAccountBalanceColumnID[i]].Add(revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]][text]);                                  

                                else
                                {
                                    count = revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].Count;
                                    revisedBalanceAccountKey2Value[sourceAccountBalanceColumnID[i]].Add(count, text);
                                    revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].Add(text, count);
                                    revisedBalanceAccountfactTable[sourceAccountBalanceColumnID[i]].Add(count);                                     
                                }
                            }
                            else
                            {
                                var text = currentOutput.key2Value[resultAccountBalanceColumnID[i]][currentOutput.factTable[resultAccountBalanceColumnID[i]][y]];

                                if (revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].ContainsKey(text)) // same master record
                                    revisedBalanceAccountfactTable[sourceAccountBalanceColumnID[i]].Add(revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]][text]);

                                else
                                {
                                    count = revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].Count;
                                    revisedBalanceAccountKey2Value[sourceAccountBalanceColumnID[i]].Add(count, text);
                                    revisedBalanceAccountValue2Key[sourceAccountBalanceColumnID[i]].Add(text, count);
                                    revisedBalanceAccountfactTable[sourceAccountBalanceColumnID[i]].Add(count);
                                }
                            }
                        }
                        
                    }
                   
                    resultFactTable.Clear();
                    resultKey2Value.Clear();
                    resultValue2Key.Clear();                   

                    for (int x = 0; x < currentOutput.columnName.Count; x++)
                    {
                        if (currentOutput.dataType[x] != "Number")
                        {
                            if (sourceAccountBalanceColumnID.Contains(x))
                            {                               
                                resultFactTable.Add(x, revisedBalanceAccountfactTable[x]);
                                resultKey2Value.Add(x, revisedBalanceAccountKey2Value[x]);
                                resultValue2Key.Add(x, revisedBalanceAccountValue2Key[x]);
                            }
                            else
                            {
                                resultFactTable.Add(x, currentOutput.factTable[x]);
                                resultKey2Value.Add(x, currentOutput.key2Value[x]);
                                resultValue2Key.Add(x, currentOutput.value2Key[x]);
                            }
                        }
                        else
                            resultFactTable.Add(x, currentOutput.factTable[x]);
                    }                  

                    currentOutput.columnName = currentOutput.columnName;
                    currentOutput.upperColumnName2ID = currentOutput.upperColumnName2ID;
                    currentOutput.dataType = currentOutput.dataType;
                    currentOutput.factTable = resultFactTable;
                    currentOutput.key2Value = resultKey2Value;
                    currentOutput.value2Key = resultValue2Key;                    

                    setSelectColumn.selectColumn = originalColumnName;
                    setSelectColumn.selectType = "Add";                  
                    currentOutput = newSelectColumn.selectColumnName(currentOutput, setSelectColumn);                   

                }
            }

            return currentOutput;
        }       
        public string amendDateByCell(string cellValue, amendDateSetting currentSetting)
        { 
            bool success = int.TryParse(cellValue, out int number);
            string currentDate = null;            

            if (success == true)
            {
                if (currentSetting.nextPeriodAddDay == -999)
                {

                    int year;
                    int month;
                    int day;

                    if (currentSetting.year == 0)
                        year = DateTime.FromOADate(number).Year;
                    else
                        year = currentSetting.year;

                    if (currentSetting.month == 0)
                        month = DateTime.FromOADate(number).Month;
                    else
                        month = currentSetting.month;

                    if (currentSetting.day == 0)
                        day = DateTime.FromOADate(number).Day;
                    else
                        day = currentSetting.day;

                    currentDate = new DateTime(year, month, day).AddDays(currentSetting.addDay).AddMonths(currentSetting.addMonth).AddYears(currentSetting.addYear).ToOADate().ToString();
                }
                else
                {
                    if (currentSetting.ruleType.ToUpper() == "REVERSEMONTHLYVOUCHER" || currentSetting.ruleType.ToUpper() == "REVERSEDAILYVOUCHER")
                    {
                        int year;
                        int month;
                        int day;

                        if (currentSetting.year == 0)
                            year = DateTime.FromOADate(number).Year;
                        else
                            year = currentSetting.year;

                        if (currentSetting.month == 0)
                            month = DateTime.FromOADate(number).Month;
                        else
                            month = currentSetting.month;

                        if (currentSetting.day == 0)
                            day = DateTime.FromOADate(number).Day;
                        else
                            day = currentSetting.day;

                        if (currentSetting.ruleType.ToUpper() == "REVERSEMONTHLYVOUCHER")
                            currentDate = new DateTime(year, month, day).AddDays(1).AddMonths(1).AddYears(0).ToOADate().ToString();

                        else if (currentSetting.ruleType.ToUpper() == "REVERSEDAILYVOUCHER")
                            currentDate = new DateTime(year, month, day).AddDays(1).AddMonths(0).AddYears(0).ToOADate().ToString();
                    }

                    else if (currentSetting.ruleType.ToUpper() == "REVERSEWEEKLYVOUCHER")
                    {
                        int currentWeek; int add_Day = 0;
                        CultureInfo myCI = new CultureInfo(currentSetting.cultureOption);
                        Calendar myCal = myCI.Calendar;
                        currentWeek = myCal.GetWeekOfYear(DateTime.FromOADate(number), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday);

                        do
                        {
                            add_Day++;

                        } while (myCal.GetWeekOfYear(DateTime.FromOADate(number + add_Day), CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday) == currentWeek);

                        currentDate = (number + add_Day).ToString();
                    }
                }

                return currentDate;
            }
            return cellValue;           
        }
    }
}
