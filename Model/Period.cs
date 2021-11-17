using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class periodSetting
    {
        public int rowThread = 100;
        public string periodDateColumn { get; set; }
        public int periodStartDayNumber { get; set; }
        public int periodStartMonthNumber { get; set; }
        public int periodStartWeekNumber { get; set; }
        public string periodType { get; set; }
        public string cultureOption { get; set; }
    }

    public class period
    {
        public LedgerRAM periodCalc(LedgerRAM currentTable, periodSetting currentSetting)
        {
            int dateColumnID = currentTable.upperColumnName2ID[currentSetting.periodDateColumn.ToUpper()];

            ConcurrentDictionary<int, List<double>> factTableMultithread = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, period> concurrentRowSegment = new ConcurrentDictionary<int, period>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();          
            List<int> rowSegment = new List<int>();            
           
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            List<double> factTablePeriodChange = new List<double>();
            List<double> factTablePeriodEnd = new List<double>();
            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();
            Dictionary<string, double> date2PeriodKey = new Dictionary<string, double>();


            double text;
            DateTime date;
            DateTime yearFirstDay;
            int periodNumber;
            int yearNumber;
            string periodText;
            int count;
            int yearDays;
            TimeSpan t;
            TimeSpan d;
            DateTime lastyearFirstDay;
            DateTime lastyearLastDay;
            int lessYear;
            DateTime currentYearFirstDay;

            if (currentSetting.periodType == null || currentSetting.periodType.ToUpper() == "MONTH")
            {
                for (int y = 0; y < currentTable.key2Value[dateColumnID].Count; y++)
                {
                    text = Convert.ToDouble(currentTable.key2Value[dateColumnID][y]);
                    date = DateTime.FromOADate(text);

                    if (1 - currentSetting.periodStartMonthNumber + date.Month <= 0)
                    {
                        periodNumber = 1 - currentSetting.periodStartMonthNumber + date.Month + 12;
                        yearNumber = date.Year - 1;
                    }
                    else
                    {
                        periodNumber = 1 - currentSetting.periodStartMonthNumber + date.Month;
                        yearNumber = date.Year;
                    }

                    if (currentSetting.periodStartMonthNumber >= 7)
                        yearNumber++;

                    if (periodNumber < 10)
                        periodText = yearNumber.ToString() + "p0" + periodNumber.ToString();
                    else
                        periodText = yearNumber.ToString() + "p" + periodNumber.ToString();

                    if (!value2Key.ContainsKey(periodText))
                    {
                        count = value2Key.Count;
                        key2Value.Add(count, periodText);
                        value2Key.Add(periodText, count);

                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], count);
                    }
                    else
                    {
                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], value2Key[periodText]);
                    }
                }
            }

            else if (currentSetting.periodType == null || currentSetting.periodType.ToUpper() == "DAY")
            {
                for (int y = 0; y < currentTable.key2Value[dateColumnID].Count; y++)
                {
                    text = Convert.ToDouble(currentTable.key2Value[dateColumnID][y]);
                    date = DateTime.FromOADate(text);
                    yearFirstDay = new DateTime(DateTime.FromOADate(text).Year, 1, 1);

                    currentYearFirstDay = new DateTime(date.Year, 1, 1);
                    currentYearFirstDay = currentYearFirstDay.AddDays(currentSetting.periodStartDayNumber - 1);

                    if (currentYearFirstDay.Month >= 3)
                        lessYear = 0;
                    else
                        lessYear = 1;

                    lastyearFirstDay = new DateTime(DateTime.FromOADate(text).Year - lessYear, 1, 1);
                    lastyearLastDay = new DateTime(DateTime.FromOADate(text).Year - lessYear, 12, 31);
                    t = date - yearFirstDay;
                    periodNumber = (int)t.TotalDays + 1;
                    d = lastyearLastDay - lastyearFirstDay;
                    yearDays = (int)d.TotalDays + 1;                   

                    if (1 - currentSetting.periodStartDayNumber + periodNumber <= 0)
                    {
                        periodNumber = 1 - currentSetting.periodStartDayNumber + periodNumber + yearDays;
                        yearNumber = date.Year - 1;
                    }
                    else
                    {
                        periodNumber = 1 - currentSetting.periodStartDayNumber + periodNumber;
                        yearNumber = date.Year;
                    }

                    if (currentSetting.periodStartDayNumber >= 183)
                        yearNumber++;

                    if (periodNumber < 10)
                        periodText = yearNumber.ToString() + "p00" + periodNumber.ToString();
                    else if (periodNumber < 100)
                        periodText = yearNumber.ToString() + "p0" + periodNumber.ToString();
                    else 
                        periodText = yearNumber.ToString() + "p" + periodNumber.ToString();                    

                    if (!value2Key.ContainsKey(periodText))
                    {
                        count = value2Key.Count;
                        key2Value.Add(count, periodText);
                        value2Key.Add(periodText, count);

                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], count);
                    }
                    else
                    {
                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], value2Key[periodText]);
                    }
                }
            }

            else if (currentSetting.periodType.ToUpper() == "WEEK")
            {
               string cultureOption = null;

               if (currentSetting.cultureOption == null)
                    cultureOption = "zh-HK";
               else
                  cultureOption = currentSetting.cultureOption;


                CultureInfo myCI = new CultureInfo(cultureOption);
                Calendar myCal = myCI.Calendar;
                int currentWeek;
                int currentYear;
                int currentMonth;
                DateTime lastYear1231;
                int lastYearTotalWeek;

                for (int y = 0; y < currentTable.key2Value[dateColumnID].Count; y++)
                {
                    text = Convert.ToDouble(currentTable.key2Value[dateColumnID][y]);
                    date = DateTime.FromOADate(text);

                    /*
                    string oaDate = "44198";
                    bool success = double.TryParse(oaDate, out double number);
                    var currentDate = DateTime.FromOADate(number);
                    */

                    currentWeek = myCal.GetWeekOfYear(date, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday);
                    currentYear = myCal.GetYear(date);
                   
                    lastYear1231 = new DateTime(currentYear - 1, 12, 31);                    

                    // Console.WriteLine(currentYear + " " + lastYear1231 + "  " + date);
                    lastYearTotalWeek = myCal.GetWeekOfYear(lastYear1231, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday);
                   // Console.WriteLine(lastYearTotalWeek);
                    currentMonth = myCal.GetMonth(date);

                    if (currentWeek >= 52 && currentMonth == 1)
                        currentYear--;

                    periodNumber = currentWeek;
                    yearNumber = currentYear;

                    // 1 - 14 + CurrentWeek  < 0
                    // 1 - 14 + CurrentWeek + LastYearTotalWeek

                    // 1 - 14 + CurrentWeek > 0
                    //  1 - 14 + CurrentWeek

                    if (1 - currentSetting.periodStartWeekNumber + currentWeek <= 0)
                    {
                        periodNumber = 1 - currentSetting.periodStartWeekNumber + currentWeek + lastYearTotalWeek;
                        yearNumber = currentYear - 1;
                    }
                    else
                    {
                        periodNumber = 1 - currentSetting.periodStartWeekNumber + currentWeek;
                        yearNumber = currentYear;
                    }

                    if (currentSetting.periodStartMonthNumber >= 26)
                        yearNumber++;                    


                    if (periodNumber < 10)
                        periodText = yearNumber.ToString() + "p0" + periodNumber.ToString();
                    else
                        periodText = yearNumber.ToString() + "p" + periodNumber.ToString();

                    if (!value2Key.ContainsKey(periodText))
                    {
                        count = value2Key.Count;
                        key2Value.Add(count, periodText);
                        value2Key.Add(periodText, count);

                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], count);
                    }
                    else
                    {
                        date2PeriodKey.Add(currentTable.key2Value[dateColumnID][y], value2Key[periodText]);
                    }
                }
            }



            rowSegment.Add(1);
            if (currentTable.factTable[0].Count > 1000)
            {
                int rowSegmentLength = Convert.ToInt32(Math.Round((double)((currentTable.factTable[0].Count - 1) / currentSetting.rowThread), 0));

                for (int y = 1; y < currentSetting.rowThread; y++)
                    rowSegment.Add(rowSegmentLength * y);

                rowSegment.Add(currentTable.factTable[0].Count);
            }
            else
            {
                rowSegment.Add(currentTable.factTable[0].Count);
            }

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new period());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {

               // if (currentSetting.periodType == null || currentSetting.periodType.ToUpper() == "MONTH")
                 factTableMultithread[currentSegment] = addPeriod(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, dateColumnID, value2Key, date2PeriodKey);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            int resultColumnID = currentTable.columnName.Count;
            factTablePeriodChange.Add(resultColumnID);
            factTablePeriodEnd.Add(resultColumnID + 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
            {
                factTablePeriodChange.AddRange(factTableMultithread[i]);
                factTablePeriodEnd.AddRange(factTableMultithread[i]);
            }

            // upperColumnName2ID.Clear();

            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();

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

            resultDataType.Add(resultColumnID, "Period");          


            if (currentSetting.periodType == null || currentSetting.periodType.ToUpper() == "MONTH")
            {
                if (!resultUpperColumnName2ID.ContainsKey("PERIOD CHANGE"))
                {
                    resultColumnName.Add(resultColumnID, "Period Change");
                    resultUpperColumnName2ID.Add("PERIOD CHANGE", resultColumnID);
                }
            }

            else if (currentSetting.periodType.ToUpper() == "DAY")
            {
                if (!resultUpperColumnName2ID.ContainsKey("DPERIOD CHANGE"))
                {
                    resultColumnName.Add(resultColumnID, "dPeriod Change");
                    resultUpperColumnName2ID.Add("DPERIOD CHANGE", resultColumnID);
                }
            }

            else if (currentSetting.periodType.ToUpper() == "WEEK")
            {
                if (!resultUpperColumnName2ID.ContainsKey("WPERIOD CHANGE"))
                {
                    resultColumnName.Add(resultColumnID, "wPeriod Change");
                    resultUpperColumnName2ID.Add("WPERIOD CHANGE", resultColumnID);
                }
            }

            resultFactTable.Add(resultColumnID, factTablePeriodChange);            
            resultKey2Value.Add(resultColumnID, key2Value);
            resultValue2Key.Add(resultColumnID, value2Key);

            resultColumnID++;

            resultDataType.Add(resultColumnID, "Period");            

            if (currentSetting.periodType == null || currentSetting.periodType.ToUpper() == "MONTH")
            {
                if (!resultUpperColumnName2ID.ContainsKey("PERIOD END"))
                {
                    resultColumnName.Add(resultColumnID, "Period End");
                    resultUpperColumnName2ID.Add("PERIOD END", resultColumnID);
                }
            }

            else if (currentSetting.periodType.ToUpper() == "DAY")
            {
                if (!resultUpperColumnName2ID.ContainsKey("DPERIOD END"))
                {
                    resultColumnName.Add(resultColumnID, "dPeriod End");
                    resultUpperColumnName2ID.Add("DPERIOD END", resultColumnID);
                }
            }

            else if (currentSetting.periodType.ToUpper() == "WEEK")
            {
                if (!resultUpperColumnName2ID.ContainsKey("WPERIOD END"))
                {
                    resultColumnName.Add(resultColumnID, "wPeriod End");
                    resultUpperColumnName2ID.Add("WPERIOD END", resultColumnID);
                }
            }

            resultFactTable.Add(resultColumnID, factTablePeriodEnd);            
            resultKey2Value.Add(resultColumnID, key2Value);
            resultValue2Key.Add(resultColumnID, value2Key);
           
            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }
        public List<double> addPeriod(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, periodSetting currentSetting, int dateColumnID, Dictionary<string, double> value2Key, Dictionary<string, double> date2PeriodKey)
        {
            List<double> factTable = new List<double>();                              

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)          
                factTable.Add(date2PeriodKey[currentTable.key2Value[dateColumnID][currentTable.factTable[dateColumnID][y]]]);                          

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return factTable;
        }
    }
}