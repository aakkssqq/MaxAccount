using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class dataTable2LedgerRAMSetting
    {    
        public int columnThread = 100;        
    }   
    public class dataTable2LedgerRAMdataFlow
    {
        public LedgerRAM dataTable2LedgerRAM(DataTable currentInput, dataTable2LedgerRAMSetting currentSetting)
        {
            /*
                for (int x = 0; x < currentInput.Columns.Count; x++)
                    Console.WriteLine(currentInput.Columns[x].ColumnName + " " + currentInput.Columns[x].DataType);

                Console.ReadLine();
            */

            LedgerRAM currentOutput = new LedgerRAM();
            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, csv2LedgerRAMDataFlow> writeColumnThread = new ConcurrentDictionary<int, csv2LedgerRAMDataFlow>();
            Dictionary<string, int> periodDict= new Dictionary<string, int>();
            periodDict.Add("PERIOD_CHANGE", 1);
            periodDict.Add("PERIOD_END", 2);
            periodDict.Add("DPERIOD_CHANGE", 3);
            periodDict.Add("DPERIOD_END", 4);            
            periodDict.Add("WPERIOD_CHANGE", 5);
            periodDict.Add("WPERIOD_END", 6);

            for (int x = 0; x < currentInput.Columns.Count; x++)
            {  
                columnName.Add(x, currentInput.Columns[x].ColumnName.Trim());
                upperColumnName2ID.Add(currentInput.Columns[x].ColumnName.Trim().ToUpper(), x);

                if (currentInput.Columns[x].DataType == Type.GetType("System.String"))
                {
                    if(periodDict.ContainsKey(currentInput.Columns[x].ColumnName.ToUpper()))
                        dataType.Add(x, "Period");
                    else
                        dataType.Add(x, "Text");
                }

                else if (currentInput.Columns[x].DataType == Type.GetType("System.DateTime"))
                    dataType.Add(x, "Date");

                else
                    dataType.Add(x, "Number");

            }

            /*
            for (int x = 0; x < currentInput.Columns.Count; x++)
            {
                if (columnName[x].ToUpper().Contains("DATE"))
                    dataType[x] = "Date";
            } 
            */

            for (int i = 0; i < dataType.Count; i++)
            {
                factTable.Add(i, new List<double>());
                key2Value.Add(i, new Dictionary<double, string>());
                value2Key.Add(i, new Dictionary<string, double>());
            }

            for (int worker = 0; worker < dataType.Count; worker++)
                writeColumnThread.TryAdd(worker, new csv2LedgerRAMDataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, dataType.Count, options, x =>
            {
                if (dataType[x] == "Number")
                {
                    factTable[x] = createLedgerRAMMeasure(x, checkThreadCompleted, currentInput);

                    if (factTable[x].Count == 0)
                        (factTable[x], key2Value[x], value2Key[x]) = createLedgerRAMDimensionKey(x, checkThreadCompleted, currentInput);
                }
                else if (dataType[x] == "Date")
                    (factTable[x], key2Value[x], value2Key[x]) = createLedgerRAMdate(x, checkThreadCompleted, currentInput);

                else
                    (factTable[x], key2Value[x], value2Key[x]) = createLedgerRAMDimensionKey(x, checkThreadCompleted, currentInput);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < dataType.Count);
                      

            Dictionary<int, string> revisedColumnName = new Dictionary<int, string>();
            Dictionary<string, int> revisedUpperColumnName2ID = new Dictionary<string, int>();

            for (int x = 0; x < columnName.Count; x++)
            {
                if (columnName[x].Contains("_"))
                {
                    revisedColumnName.Add(x, columnName[x].Replace("Year_End$", "Year End:").Replace("_", " ").Trim());
                    revisedUpperColumnName2ID.Add(columnName[x].Replace("Year_End$", "Year End:").Replace("_", " ").Trim().ToUpper(), x);
                }

                else if (columnName[x] == "DC")
                {
                    revisedColumnName.Add(x, "D/C");
                    revisedUpperColumnName2ID.Add("D/C", x);
                }

                else
                {
                    revisedColumnName.Add(x, columnName[x].Trim());
                    revisedUpperColumnName2ID.Add(columnName[x].Trim().ToUpper(), x);
                }
            }
            
            currentOutput.factTable = new Dictionary<int, List<double>>(factTable);
            currentOutput.key2Value = new Dictionary<int, Dictionary<double, string>>(key2Value);
            currentOutput.value2Key = new Dictionary<int, Dictionary<string, double>>(value2Key);
            currentOutput.columnName = revisedColumnName;
            currentOutput.upperColumnName2ID = revisedUpperColumnName2ID;
            currentOutput.dataType = dataType;

            return currentOutput;
        }

        public (List<double> factTable, Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) createLedgerRAMdate(int columnID, ConcurrentQueue<int> checkThreadCompleted, DataTable currentInput)
        {
            List<double> factTable = new List<double>();
            StringBuilder cellValue = new StringBuilder();
            factTable.Add(columnID); // first record is column id              
            double count;
            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            for (int y = 0; y < currentInput.Rows.Count; y++)
            {
                string text = currentInput.Rows[y].Field<DateTime>(columnID).ToOADate().ToString();

                if (text.Length == 0)
                    cellValue.Append("null");

                if (value2Key.ContainsKey(text)) // same master record
                    factTable.Add(value2Key[text]);

                else // add new master record
                {
                    count = value2Key.Count;
                    key2Value.Add(count, text);
                    value2Key.Add(text, count);
                    factTable.Add(count);
                }
            }

            checkThreadCompleted.Enqueue(columnID);
            return (factTable, key2Value, value2Key);
        }
        public (List<double> factTable, Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) createLedgerRAMDimensionKey(int columnID, ConcurrentQueue<int> checkThreadCompleted, DataTable currentInput)
        {
            List<double> factTable = new List<double>();
            StringBuilder cellValue = new StringBuilder();
            factTable.Add(columnID); // first record is column id              
            double count;
            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            for (int y = 0; y < currentInput.Rows.Count; y++)
            {
                string text = currentInput.Rows[y].Field<string>(columnID);

                if (text.Length == 0)
                    cellValue.Append("null");

                if (value2Key.ContainsKey(text)) // same master record
                    factTable.Add(value2Key[text]);

                else // add new master record
                {
                    count = value2Key.Count;
                    key2Value.Add(count, text);
                    value2Key.Add(text, count);
                    factTable.Add(count);
                }
            }

            checkThreadCompleted.Enqueue(columnID);
            return (factTable, key2Value, value2Key);
        }
        public List<double> createLedgerRAMMeasure(int columnID, ConcurrentQueue<int> checkThreadCompleted, DataTable currentInput)
        {
            List<double> factTable = new List<double>();
            factTable.Add(columnID); // first record is column id    

            if (currentInput.Columns[columnID].DataType.Name.ToString() == "Double")
                for (int y = 0; y < currentInput.Rows.Count; y++)
                    factTable.Add(currentInput.Rows[y].Field<double>(columnID));

            if (currentInput.Columns[columnID].DataType.Name.ToString() == "Int32")
                for (int y = 0; y < currentInput.Rows.Count; y++)
                    factTable.Add(currentInput.Rows[y].Field<Int32>(columnID));     

            checkThreadCompleted.Enqueue(columnID);
            return factTable;
        }     
    }
}
