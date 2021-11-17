using System;
using System.Collections.Generic;
using System.Globalization;

namespace MaxAccount
{
    public class amendDateFormatSetting
    {
        public int rowThread = 100;
        public string sourceDataName { get; set; }
        public string sourceDateFormat { get; set; }
        public string resultDateFormat { get; set; }
        public List<string> columnName { get; set; }
    }

    public class amendDateFormat
    {
        public LedgerRAM amendDateFormatByTable(LedgerRAM currentTable, amendDateFormatSetting currentSetting)
        { 
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();

            List<int> dateColumnID = new List<int>();

            bool isExecute = true;

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.dataType[x] == "Date")
                {
                    dateColumnID.Add(x);                   
                    key2Value.Add(x, new Dictionary<double, string>());
                    value2Key.Add(x, new Dictionary<string, double>());
                }
            }

            if (currentSetting.sourceDateFormat.ToUpper() == "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        bool success = double.TryParse(pair.Value, out double number);

                        if(success == false)
                            isExecute = false;

                        if (success == true)
                        {
                            var currentDate = DateTime.FromOADate(number).ToString(currentSetting.resultDateFormat);
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);
                            value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                    }                    
                }
            }

            DateTime date;
            if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() == "OLEAUTOMATIONDATE")
            {
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        if (DateTime.TryParseExact(pair.Value, currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                        {
                            var currentDate = date.ToOADate().ToString();
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);
                            value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                        else
                            isExecute = false;
                    }
                }
            }

            if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {   
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        if (DateTime.TryParseExact(pair.Value, currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                        {
                            var currentDate = date.ToString(currentSetting.resultDateFormat);
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);
                            value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                        else
                            isExecute = false;
                    }
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

            if (isExecute == true)
            {
                
                currentOutput.key2Value = resultKey2Value;
                currentOutput.value2Key = resultValue2Key;
            }
            else
            {
               
                currentOutput.key2Value = currentTable.key2Value;
                currentOutput.value2Key = currentTable.value2Key;
            }


            return currentOutput;
        }
        public LedgerRAM amendDateFormatByColumn(LedgerRAM currentTable, amendDateFormatSetting currentSetting)
        {
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();           
           
            List<int> dateColumnID = new List<int>();

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);          

            for (int x = 0; x < currentSetting.columnName.Count; x++)         
                if (upperColumnName2ID.ContainsKey(currentSetting.columnName[x].ToUpper()))
                    dateColumnID.Add(upperColumnName2ID[currentSetting.columnName[x].ToUpper()]);

            bool isExecute = true;

            if (currentSetting.sourceDateFormat.ToUpper() == "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    key2Value.Add(dateColumnID[x], new Dictionary<double, string>());
                    value2Key.Add(dateColumnID[x], new Dictionary<string, double>());

                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        bool success = double.TryParse(pair.Value, out double number);

                        if (success == false)
                            isExecute = false;

                        if (success == true)
                        {
                            var currentDate = DateTime.FromOADate(number).ToString(currentSetting.resultDateFormat);
                          
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);

                            if (!value2Key[dateColumnID[x]].ContainsKey(currentDate))
                                value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                    }
                }
            }

            DateTime date;
            if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() == "OLEAUTOMATIONDATE")
            {
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    key2Value.Add(dateColumnID[x], new Dictionary<double, string>());
                    value2Key.Add(dateColumnID[x], new Dictionary<string, double>());

                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        if (DateTime.TryParseExact(pair.Value, currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                        {
                            var currentDate = date.ToOADate().ToString();                           
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);

                            if (!value2Key[dateColumnID[x]].ContainsKey(currentDate))
                                value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                        else
                            isExecute = false;
                    }
                }
            }

            if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {
                for (int x = 0; x < dateColumnID.Count; x++)
                {
                    key2Value.Add(dateColumnID[x], new Dictionary<double, string>());
                    value2Key.Add(dateColumnID[x], new Dictionary<string, double>());

                    foreach (var pair in currentTable.key2Value[dateColumnID[x]])
                    {
                        if (DateTime.TryParseExact(pair.Value, currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                        {
                            var currentDate = date.ToString(currentSetting.resultDateFormat);
                            key2Value[dateColumnID[x]].Add(pair.Key, currentDate);

                            if (!value2Key[dateColumnID[x]].ContainsKey(currentDate))
                                value2Key[dateColumnID[x]].Add(currentDate, pair.Key);
                        }
                        else
                            isExecute = false;
                    }
                }
            }          

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.dataType[x] != "Number")
                {
                    if (dateColumnID.Contains(x))
                    {
                        resultKey2Value.Add(x, key2Value[x]);
                        resultValue2Key.Add(x, value2Key[x]);
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

            if (isExecute == true)
            {
                currentOutput.key2Value = resultKey2Value;
                currentOutput.value2Key = resultValue2Key;
            }
            else
            {
                currentOutput.key2Value = currentTable.key2Value;
                currentOutput.value2Key = currentTable.value2Key;
            }

            return currentOutput;
        }
        public string amendDateFormatByCell(string cellValue,  amendDateFormatSetting currentSetting)
        {
            string currentDate = null;
            DateTime date;

            if (currentSetting.sourceDateFormat.ToUpper() == "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {
                bool success = double.TryParse(cellValue, out double number);

                if(success == true)
                    currentDate = DateTime.FromOADate(number).ToString(currentSetting.resultDateFormat);
                else
                    currentDate = cellValue;
            }
            else if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() == "OLEAUTOMATIONDATE")
            {
                if (DateTime.TryParseExact(cellValue, currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                    currentDate = date.ToOADate().ToString();
                else
                    currentDate = cellValue;
            }
            else if (currentSetting.sourceDateFormat.ToUpper() != "OLEAUTOMATIONDATE" && currentSetting.resultDateFormat.ToUpper() != "OLEAUTOMATIONDATE")
            {
                if (DateTime.TryParseExact(cellValue.ToString(), currentSetting.sourceDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                    currentDate = date.ToString(currentSetting.resultDateFormat);
                else
                    currentDate = cellValue;
            }            
           
            return currentDate;
        }
    }
}
