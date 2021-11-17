using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class amortizationSetting
    {
        public string assetID { get; set; }
        public double acquisition { get; set; }
        public double residual { get; set; }
        public double totalTenor { get; set; }
        public string startDate { get; set; }
        public string endDate { get; set; }
        public List<string> amortizationMethod { get; set; }

        public int rowThread = 100;
    }

    public class amortization
    {
        public LedgerRAM amortizationProcess(LedgerRAM currentTable, amortizationSetting currentSetting)
        {            
            ConcurrentDictionary<string, LedgerRAM> ramStore = new ConcurrentDictionary<string, LedgerRAM>();

            List<string> tableName = new List<string>();

            LedgerRAM currentOutput = new LedgerRAM();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, amortization> concurrentSegmentAddress = new ConcurrentDictionary<int, amortization>();          

            for (int y = 1; y < currentTable.factTable[0].Count; y++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey("ASSETID"))
                {
                    currentSetting.assetID = currentTable.key2Value[currentTable.upperColumnName2ID["ASSETID"]][currentTable.factTable[currentTable.upperColumnName2ID["ASSETID"]][y]];                   
                }               
            }

            for (int worker = 1; worker < currentTable.factTable[0].Count; worker++)
            {              
                 concurrentSegmentAddress.TryAdd(worker, new amortization());
            }
            
            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(1, currentTable.factTable[0].Count, options, row =>                      
            {  
                ramStore.TryAdd(row.ToString(), concurrentSegmentAddress[row].amortizationThread(row, checkThreadCompleted, currentTable, currentSetting));
            });

            do
            {
                Thread.Sleep(5);
              
            } while (checkThreadCompleted.Count < currentTable.factTable[0].Count - 1);            

            foreach (var items in ramStore)           
                tableName.Add(items.Key);
           
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            Dictionary<string, LedgerRAM> backupRamStore = new Dictionary<string, LedgerRAM>();

            copyTable newcopyTable = new copyTable();

            foreach (var pair in tableName)            
                backupRamStore.Add(pair, newcopyTable.copyTableProcess(ramStore[pair]));          

            string text = null;
            int count;

            for (int x = 0; x < ramStore["1"].columnName.Count; x++)
            {
                resultColumnName.Add(x, ramStore["1"].columnName[x]);              
                resultUpperColumnName2ID.Add(ramStore["1"].columnName[x].ToUpper(), x);
                resultDataType.Add(x, ramStore["1"].dataType[x]);
                resultFactTable.Add(x, new List<double>());
                resultFactTable[x].Add(x);

                if (ramStore["1"].dataType[x] == "Number")
                {
                    foreach (var pair in ramStore)
                    {
                        backupRamStore[pair.Key.ToString()].factTable[x].RemoveAt(0);                      
                        resultFactTable[x].AddRange(backupRamStore[pair.Key.ToString()].factTable[x]);
                    }
                }
                else
                {
                    resultKey2Value.Add(x, new Dictionary<double, string>());
                    resultValue2Key.Add(x, new Dictionary<string, double>());               

                    foreach (var pair in ramStore)
                    {
                        for (int y = 1; y < ramStore[pair.Key.ToString()].factTable[x].Count; y++)
                        {
                            text = ramStore[pair.Key.ToString()].key2Value[x][ramStore[pair.Key.ToString()].factTable[x][y]].ToString();                          

                            if (resultValue2Key[x].ContainsKey(text))
                                resultFactTable[x].Add(resultValue2Key[x][text]);

                            else
                            {
                                count = resultValue2Key[x].Count;
                                resultKey2Value[x].Add(count, text);
                                resultValue2Key[x].Add(text, count);
                                resultFactTable[x].Add(count);
                            }
                        }
                    }
                }
            }
          
            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }

        public LedgerRAM amortizationThread(int row, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, amortizationSetting _currentSetting)
        {
            amortizationSetting currentSetting = new amortizationSetting();
            bool residualExist = false;           

            currentSetting.amortizationMethod = _currentSetting.amortizationMethod;

            if (currentTable.upperColumnName2ID.ContainsKey("ASSETID"))
                currentSetting.assetID = currentTable.key2Value[currentTable.upperColumnName2ID["ASSETID"]][currentTable.factTable[currentTable.upperColumnName2ID["ASSETID"]][row]];

            if (currentTable.upperColumnName2ID.ContainsKey("ACQUISITION"))           
                currentSetting.acquisition = currentTable.factTable[currentTable.upperColumnName2ID["ACQUISITION"]][row];

            if (currentTable.upperColumnName2ID.ContainsKey("RESIDUAL"))
            {
                currentSetting.residual = currentTable.factTable[currentTable.upperColumnName2ID["RESIDUAL"]][row];
                residualExist = true;               
            }

            if (currentTable.upperColumnName2ID.ContainsKey("TOTALTENOR"))
                currentSetting.totalTenor = currentTable.factTable[currentTable.upperColumnName2ID["TOTALTENOR"]][row];

            if (currentTable.upperColumnName2ID.ContainsKey("STARTDATE"))
                currentSetting.startDate = currentTable.key2Value[currentTable.upperColumnName2ID["STARTDATE"]][currentTable.factTable[currentTable.upperColumnName2ID["STARTDATE"]][row]];

            if (currentTable.upperColumnName2ID.ContainsKey("ENDDATE"))         
                currentSetting.endDate = currentTable.key2Value[currentTable.upperColumnName2ID["ENDDATE"]][currentTable.factTable[currentTable.upperColumnName2ID["ENDDATE"]][row]];

            LedgerRAM currentOutput = new LedgerRAM();

            amortization newAmortization = new amortization();         

            currentOutput = newAmortization.amortizeOneAsset(currentSetting, residualExist);         

            checkThreadCompleted.Enqueue(row);

            return currentOutput;
        }

        public LedgerRAM amortizeOneAsset(amortizationSetting currentSetting, bool residualExist)
        {
            int decimalPlace = 999;
            bool success;
            int beforeDisposalTenor = 0;
            double disposalAmount = 0;

            for (int i = 0; i < currentSetting.amortizationMethod.Count; i++)
            {
                if (currentSetting.amortizationMethod[i].ToUpper().Contains("ROUND"))
                {
                    success = int.TryParse(currentSetting.amortizationMethod[i].Substring(5, 1), out decimalPlace);
                }
            }

            DateTime startDate = new DateTime(1900, 01, 01);
            DateTime _startDate = new DateTime(1900, 01, 01);
            DateTime endDate = new DateTime(1900, 01, 01);

            string[] formats = { "MMM-dd-yyyy", "dd-MMM-yyyy", "d-MMM-yyyy", "d-MMM-yy" };

            DateTime dateValue;

            foreach (string dateStringFormat in formats)
            {
                if (DateTime.TryParseExact(currentSetting.startDate, dateStringFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out dateValue))
                    startDate = dateValue;

                if (DateTime.TryParseExact(currentSetting.endDate, dateStringFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out dateValue))
                    endDate = dateValue;
            }

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            Dictionary<string, int> columnName2ID = new Dictionary<string, int>();

            int columnID = 0;

            if (currentSetting.assetID != null)
            {
                resultDataType.Add(columnID, "Text");
                resultColumnName.Add(columnID, "AssetID");
                resultUpperColumnName2ID.Add("ASSETID", columnID);
                resultFactTable.Add(columnID, new List<double>());
                resultKey2Value.Add(columnID, new Dictionary<double, string>());
                resultValue2Key.Add(columnID, new Dictionary<string, double>());
                columnName2ID.Add("AssetID", columnID);
                columnID++;
            }

            if (currentSetting.totalTenor != 0)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "Tenor");
                resultUpperColumnName2ID.Add("TENOR", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("Tenor", columnID);
                columnID++;
            }

            if (currentSetting.startDate != null)
            {
                resultDataType.Add(columnID, "Date");
                resultColumnName.Add(columnID, "Date");
                resultUpperColumnName2ID.Add("DATE", columnID);
                resultFactTable.Add(columnID, new List<double>());
                resultKey2Value.Add(columnID, new Dictionary<double, string>());
                resultValue2Key.Add(columnID, new Dictionary<string, double>());
                columnName2ID.Add("Date", columnID);
                columnID++;
            }

            if (currentSetting.acquisition != 0)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "Acquisition");
                resultUpperColumnName2ID.Add("ACQUISITION", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("Acquisition", columnID);
                columnID++;
            }

            if (currentSetting.residual != 0 || residualExist == true)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "Residual");
                resultUpperColumnName2ID.Add("RESIDUAL", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("Residual", columnID);
                columnID++;
            }

            if (currentSetting.amortizationMethod != null)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "Amortization");
                resultUpperColumnName2ID.Add("AMORTIZATION", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("Amortization", columnID);
                columnID++;
            }

            if (currentSetting.amortizationMethod != null)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "AccAmortization");
                resultUpperColumnName2ID.Add("ACCAMORTIZATION", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("AccAmortization", columnID);
                columnID++;
            }

            if (currentSetting.endDate != null)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "Disposal");
                resultUpperColumnName2ID.Add("DISPOSAL", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("Disposal", columnID);
                columnID++;
            }

            if (currentSetting.acquisition != 0)
            {
                resultDataType.Add(columnID, "Number");
                resultColumnName.Add(columnID, "NetBookValue");
                resultUpperColumnName2ID.Add("NETBOOKVALUE", columnID);
                resultFactTable.Add(columnID, new List<double>());
                columnName2ID.Add("NetBookValue", columnID);
            }

            for (int x = 0; x < resultColumnName.Count; x++)
                resultFactTable[x].Add(x);       

            int count;
            string text;
            double currentDate;
            double endDateAtLastDay;
            double accAmortization;

            if (currentSetting.assetID != null)
            {
                text = currentSetting.assetID;

                if (resultValue2Key[columnName2ID["AssetID"]].ContainsKey(text))
                    resultFactTable[columnName2ID["AssetID"]].Add(resultValue2Key[0][text]);
                else
                {
                    count = resultValue2Key[columnName2ID["AssetID"]].Count;
                    resultKey2Value[columnName2ID["AssetID"]].Add(count, text);
                    resultValue2Key[columnName2ID["AssetID"]].Add(text, count);
                    resultFactTable[columnName2ID["AssetID"]].Add(count);
                }
            }

            resultFactTable[columnName2ID["Tenor"]].Add(0);

            text = startDate.ToOADate().ToString();

            if (resultValue2Key[columnName2ID["Date"]].ContainsKey(text))
                resultFactTable[columnName2ID["Date"]].Add(resultValue2Key[2][text]);
            else
            {
                count = resultValue2Key[columnName2ID["Date"]].Count;
                resultKey2Value[columnName2ID["Date"]].Add(count, text);
                resultValue2Key[columnName2ID["Date"]].Add(text, count);
                resultFactTable[columnName2ID["Date"]].Add(count);
            }

            resultFactTable[columnName2ID["Acquisition"]].Add(currentSetting.acquisition);

            if (currentSetting.residual != 0 || residualExist == true)
                 resultFactTable[columnName2ID["Residual"]].Add(currentSetting.residual);           

            resultFactTable[columnName2ID["Amortization"]].Add(0);
            resultFactTable[columnName2ID["AccAmortization"]].Add(0);

            if (currentSetting.endDate != null)            
                resultFactTable[columnName2ID["Disposal"]].Add(0);

            resultFactTable[columnName2ID["NetBookValue"]].Add(currentSetting.acquisition);            

            for (int y = 1; y <= currentSetting.totalTenor; y++)
            {
                if (currentSetting.assetID != null)
                {
                    text = currentSetting.assetID;                  

                    if (resultValue2Key[columnName2ID["AssetID"]].ContainsKey(text))
                        resultFactTable[columnName2ID["AssetID"]].Add(resultValue2Key[0][text]);
                    else
                    {
                        count = resultValue2Key[columnName2ID["AssetID"]].Count;
                        resultKey2Value[columnName2ID["AssetID"]].Add(count, text);
                        resultValue2Key[columnName2ID["AssetID"]].Add(text, count);
                        resultFactTable[columnName2ID["AssetID"]].Add(count);
                    }
                }

                resultFactTable[columnName2ID["Tenor"]].Add(y);

                currentDate = new DateTime(startDate.Year, startDate.Month, 01).AddMonths(y).AddDays(-1).ToOADate();
                endDateAtLastDay = new DateTime(endDate.Year, endDate.Month, 01).AddDays(-1).ToOADate();

                text = currentDate.ToString();               

                if (resultValue2Key[columnName2ID["Date"]].ContainsKey(text)) 
                    resultFactTable[columnName2ID["Date"]].Add(resultValue2Key[2][text]);
                else
                {
                    count = resultValue2Key[columnName2ID["Date"]].Count;
                    resultKey2Value[columnName2ID["Date"]].Add(count, text);
                    resultValue2Key[columnName2ID["Date"]].Add(text, count);
                    resultFactTable[columnName2ID["Date"]].Add(count);
                }                

                if (beforeDisposalTenor == 0 || y < beforeDisposalTenor)
                    resultFactTable[columnName2ID["Acquisition"]].Add(currentSetting.acquisition);
                else
                    resultFactTable[columnName2ID["Acquisition"]].Add(0);

                if (currentSetting.residual != 0 || residualExist == true)
                {
                    if (beforeDisposalTenor == 0 || y < beforeDisposalTenor)
                        resultFactTable[columnName2ID["Residual"]].Add(currentSetting.residual);
                    else
                        resultFactTable[columnName2ID["Residual"]].Add(0);
                }

                if (currentSetting.residual != 0)
                    accAmortization = (currentSetting.acquisition - currentSetting.residual) * y / currentSetting.totalTenor;
                else
                    accAmortization = currentSetting.acquisition * y / currentSetting.totalTenor;

                if (decimalPlace == 999)
                {
                    if (beforeDisposalTenor == 0 || y < beforeDisposalTenor)
                    {
                        resultFactTable[columnName2ID["AccAmortization"]].Add(accAmortization);

                        if (y == 1)
                            resultFactTable[columnName2ID["Amortization"]].Add(resultFactTable[5][y + 1]);
                        else
                            resultFactTable[columnName2ID["Amortization"]].Add(resultFactTable[5][y + 1] - resultFactTable[5][y]);

                        resultFactTable[columnName2ID["NetBookValue"]].Add(currentSetting.acquisition - accAmortization);
                    }
                    else
                    {
                        resultFactTable[columnName2ID["AccAmortization"]].Add(0);
                        resultFactTable[columnName2ID["Amortization"]].Add(0);
                        resultFactTable[columnName2ID["NetBookValue"]].Add(0);
                    }

                    if (currentSetting.endDate != null)
                    {
                        if (beforeDisposalTenor == 0)
                            resultFactTable[columnName2ID["Disposal"]].Add(0);
                        else
                        {
                            resultFactTable[columnName2ID["Disposal"]].Add(disposalAmount);
                            disposalAmount = 0;
                        }

                        if (endDateAtLastDay == currentDate)
                        {
                            beforeDisposalTenor = y;
                            disposalAmount = currentSetting.acquisition - accAmortization;
                        }
                    }
                }
                else
                {
                    if (beforeDisposalTenor == 0 || y < beforeDisposalTenor)
                    {
                        resultFactTable[columnName2ID["AccAmortization"]].Add(Math.Round((double)(accAmortization), decimalPlace));

                        if (y == 1)
                            resultFactTable[columnName2ID["Amortization"]].Add(Math.Round((double)(resultFactTable[columnName2ID["AccAmortization"]][y + 1]), decimalPlace));
                        else
                            resultFactTable[columnName2ID["Amortization"]].Add(Math.Round((double)(resultFactTable[columnName2ID["AccAmortization"]][y + 1] - resultFactTable[columnName2ID["AccAmortization"]][y]), decimalPlace));

                        resultFactTable[columnName2ID["NetBookValue"]].Add(Math.Round((double)(currentSetting.acquisition - accAmortization), decimalPlace));
                    }
                    else
                    {
                        resultFactTable[columnName2ID["AccAmortization"]].Add(0);
                        resultFactTable[columnName2ID["Amortization"]].Add(0);
                        resultFactTable[columnName2ID["NetBookValue"]].Add(0);
                    }

                    if (currentSetting.endDate != null)
                    {
                        if (beforeDisposalTenor == 0)
                            resultFactTable[columnName2ID["Disposal"]].Add(0);
                        else
                        {
                            resultFactTable[columnName2ID["Disposal"]].Add(Math.Round((double)(disposalAmount), decimalPlace));
                            disposalAmount = 0;
                        }

                        if (endDateAtLastDay == currentDate)
                        {
                            beforeDisposalTenor = y;
                            disposalAmount = Math.Round((double)(currentSetting.acquisition - accAmortization), decimalPlace);
                        }
                    }
                }
            }

           
            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }

    }
}