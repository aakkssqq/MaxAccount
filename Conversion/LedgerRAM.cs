using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace MaxAccount
{
    public class csv2DataTablesetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
    }

    public class csv2HTMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
    }

    public class csv2JSONsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string tableName { get; set; }
    }

    public class csv2XMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string tableName { get; set; }
    }

    public class dataTable2CSVsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string separator = ",";
    }

    public class dataTable2HTMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
    }

    public class dataTable2JSONsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string tableName { get; set; }
    }

    public class dataTable2XMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string tableName { get; set; }
    }
    public class LedgerRAM
    {
        public int validateRow { get; set; } // number of rows for validation
        public Dictionary<int, int> tableColumnCountExceptionList { get; set; } // record non-qualified CSV info, first row is number of cell for header row         
        public int fileByteLength { get; set; } // total number of bytes of a file bytestream       
        public Dictionary<int, string> dataType { get; set; }  // date type: date, text, number
        public Dictionary<int, string> columnName { get; set; }  // assume first row is column name
        public Dictionary<string, int> upperColumnName2ID { get; set; }  // assume first row is column name
        public Dictionary<int, List<double>> factTable { get; set; } // maximum number of keys for each dimension: 65,535
        public Dictionary<int, Dictionary<double, string>> key2Value { get; set; } // key to value lookup
        public Dictionary<int, Dictionary<string, double>> value2Key { get; set; } // value to key loopup
        public Dictionary<int, List<string>> crosstabHeader { get; set; } // x column name and value
        public List<string> crosstabNumberHeader { get; set; } // e.g. Sum:Base Amount       
        public Dictionary<int, int> matchedRow { get; set; }
        public bool isPeriodEndExist { get; set; }

        public Dictionary<string, int> convertColumnName2Upper(LedgerRAM currentTable)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            return upperColumnName2ID;
        }

        public Dictionary<int, string> cutColumnNamePrefix(LedgerRAM currentTable)
        {
            Dictionary<int, string> columnName = new Dictionary<int, string>();           

            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                string col = currentTable.columnName[i].ToString();

                if (col.Contains("@"))
                {
                    var start = currentTable.columnName[i].IndexOf("@") + 1;
                    var length = currentTable.columnName[i].Length - start;
                    columnName.Add(i, currentTable.columnName[i].Substring(start, length));
                }
                else
                    columnName.Add(i, currentTable.columnName[i]);

            }
            return columnName;
        }

        public LedgerRAM csv2LedgerRAM(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentInput)
        {
            csv2LedgerRAMDataFlow currentProcess = new csv2LedgerRAMDataFlow();
            LedgerRAM currentOutput = currentProcess.csv2LedgerRAM(ramStore, currentInput);
            return currentOutput;
        } 

        public DataTable csv2DataTable(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentInput, csv2DataTablesetting setCSV2DataTable)
        {
            // csv to LedgerRAM
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
            setCSV2LedgerRAM.columnThread = setCSV2DataTable.columnThread;
            setCSV2LedgerRAM.rowThread = setCSV2DataTable.rowThread;
            csv2LedgerRAMDataFlow process1 = new csv2LedgerRAMDataFlow();
            LedgerRAM output1 = process1.csv2LedgerRAM(ramStore, currentInput);

            // LedgerRAM to DataTable
            LedgerRAM2DataTablesetting setLedgerRAM2DataTable = new LedgerRAM2DataTablesetting();
            setLedgerRAM2DataTable.rowThread = setCSV2DataTable.rowThread;            
            LedgerRAM2DataTabledataFlow process2 = new LedgerRAM2DataTabledataFlow();
            DataTable output2 = process2.LedgerRAM2DataTable(output1, setLedgerRAM2DataTable);

            return output2;
        }

        public StringBuilder csv2HTML(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentInput, csv2HTMLsetting setCSV2HTML)
        {
            // csv to LedgerRAM
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
            setCSV2LedgerRAM.columnThread = setCSV2HTML.columnThread;
            setCSV2LedgerRAM.rowThread = setCSV2HTML.rowThread;
            csv2LedgerRAMDataFlow process1 = new csv2LedgerRAMDataFlow();
            LedgerRAM output1 = process1.csv2LedgerRAM(ramStore, currentInput);

            // LedgerRAM to HTML
            LedgerRAM2HTMLsetting setLedgerRAM2HTML = new LedgerRAM2HTMLsetting();            
            setLedgerRAM2HTML.rowThread = setCSV2HTML.rowThread;           
            LedgerRAM2HTMLdataFlow process2 = new LedgerRAM2HTMLdataFlow();
            StringBuilder output2 = process2.LedgerRAM2HTML(output1, setLedgerRAM2HTML);

            return output2;
        }

        public StringBuilder csv2JSON(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentInput, csv2JSONsetting setCSV2JSON)
        {
            // csv to LedgerRAM
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
            setCSV2LedgerRAM.columnThread = setCSV2JSON.columnThread;
            setCSV2LedgerRAM.rowThread = setCSV2JSON.rowThread;
            csv2LedgerRAMDataFlow process1 = new csv2LedgerRAMDataFlow();
            LedgerRAM output1 = process1.csv2LedgerRAM(ramStore, currentInput);

            // LedgerRAM to JSON
            LedgerRAM2JSONsetting setLedgerRAM2JSON = new LedgerRAM2JSONsetting();
            setLedgerRAM2JSON.rowThread = setCSV2JSON.rowThread;            
            setLedgerRAM2JSON.tableName = setCSV2JSON.tableName;                        
            LedgerRAM2JSONdataFlow process2 = new LedgerRAM2JSONdataFlow();
            StringBuilder output2 = process2.LedgerRAM2JSON(output1, setLedgerRAM2JSON);

            return output2;
        }

        public StringBuilder csv2XML(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentInput, csv2XMLsetting setCSV2XML)
        {
            // csv to LedgerRAM
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
            setCSV2LedgerRAM.columnThread = setCSV2XML.columnThread;
            setCSV2LedgerRAM.rowThread = setCSV2XML.rowThread;
            csv2LedgerRAMDataFlow process1 = new csv2LedgerRAMDataFlow();
            LedgerRAM output1 = process1.csv2LedgerRAM(ramStore, currentInput);

            // LedgerRAM to XML
            LedgerRAM2XMLsetting setLedgerRAM2XML = new LedgerRAM2XMLsetting();
            setLedgerRAM2XML.rowThread = setCSV2XML.rowThread;            
            setLedgerRAM2XML.tableName = setCSV2XML.tableName;
            LedgerRAM2XMLdataFlow process2 = new LedgerRAM2XMLdataFlow();
            StringBuilder output2 = process2.LedgerRAM2XML(output1, setLedgerRAM2XML);

            return output2;
        }       
       
        public StringBuilder LedgerRAM2CSV(LedgerRAM currentInput, LedgerRAM2CSVsetting currentSetting)
        {
            LedgerRAM2CSVdataFlow currentProcess = new LedgerRAM2CSVdataFlow();
            StringBuilder currentOutput = currentProcess.LedgerRAM2CSV(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder LedgerRAM2JSON(LedgerRAM currentInput, LedgerRAM2JSONsetting currentSetting)
        {
            LedgerRAM2JSONdataFlow currentProcess = new LedgerRAM2JSONdataFlow();
            StringBuilder currentOutput = currentProcess.LedgerRAM2JSON(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder LedgerRAM2XML(LedgerRAM currentInput, LedgerRAM2XMLsetting currentSetting)
        {
            LedgerRAM2XMLdataFlow currentProcess = new LedgerRAM2XMLdataFlow();
            StringBuilder currentOutput = currentProcess.LedgerRAM2XML(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder LedgerRAM2HTML(LedgerRAM currentInput, LedgerRAM2HTMLsetting currentSetting)
        {
            LedgerRAM2HTMLdataFlow currentProcess = new LedgerRAM2HTMLdataFlow();
            StringBuilder currentOutput = currentProcess.LedgerRAM2HTML(currentInput, currentSetting);
            return currentOutput;
        }
        public DataTable LedgerRAM2DataTable(LedgerRAM currentInput, LedgerRAM2DataTablesetting currentSetting)
        {
            LedgerRAM2DataTabledataFlow currentProcess = new LedgerRAM2DataTabledataFlow();
            DataTable currentOutput = currentProcess.LedgerRAM2DataTable(currentInput, currentSetting);
            return currentOutput;
        }
        public LedgerRAM dataTable2LedgerRAM(DataTable currentInput, dataTable2LedgerRAMSetting currentSetting)
        {
            dataTable2LedgerRAMdataFlow currentProcess = new dataTable2LedgerRAMdataFlow();
            LedgerRAM currentOutput = currentProcess.dataTable2LedgerRAM(currentInput, currentSetting);
            return currentOutput;
        }        
        public StringBuilder dataTable2CSV(DataTable currentInput, dataTable2CSVsetting setDataTable2CSV)
        {  
            // DataTable to LedgerRAM
            dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
            setDataTable2LedgerRAM.columnThread = setDataTable2CSV.columnThread;
            dataTable2LedgerRAMdataFlow process1 = new dataTable2LedgerRAMdataFlow();
            LedgerRAM output1 = process1.dataTable2LedgerRAM(currentInput, setDataTable2LedgerRAM);

            // LedgerRAM to CSV
            LedgerRAM2CSVsetting setLedgerRAM2CSV = new LedgerRAM2CSVsetting();
            setLedgerRAM2CSV.separator = setDataTable2CSV.separator;           
            setLedgerRAM2CSV.rowThread = setDataTable2CSV.rowThread;            
            LedgerRAM2CSVdataFlow process2 = new LedgerRAM2CSVdataFlow();
            StringBuilder output2 = process2.LedgerRAM2CSV(output1, setLedgerRAM2CSV);

            return output2;
        }
        public StringBuilder dataTable2HTML(DataTable currentInput, dataTable2HTMLsetting setDataTable2HTML)
        {
            // DataTable to LedgerRAM
            dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
            setDataTable2LedgerRAM.columnThread = setDataTable2HTML.columnThread;
            dataTable2LedgerRAMdataFlow process1 = new dataTable2LedgerRAMdataFlow();
            LedgerRAM output1 = process1.dataTable2LedgerRAM(currentInput, setDataTable2LedgerRAM);

            // LedgerRAM to HTML
            LedgerRAM2HTMLsetting setLedgerRAM2HTML = new LedgerRAM2HTMLsetting();            
            setLedgerRAM2HTML.rowThread = setDataTable2HTML.rowThread;
            LedgerRAM2HTMLdataFlow process2 = new LedgerRAM2HTMLdataFlow();
            StringBuilder output2 = process2.LedgerRAM2HTML(output1, setLedgerRAM2HTML);

            return output2;
        }
        public StringBuilder dataTable2JSON(DataTable currentInput, dataTable2JSONsetting setDataTable2JSON)
        {
            // DataTable to LedgerRAM
            dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
            setDataTable2LedgerRAM.columnThread = setDataTable2JSON.columnThread;
            dataTable2LedgerRAMdataFlow process1 = new dataTable2LedgerRAMdataFlow();
            LedgerRAM output1 = process1.dataTable2LedgerRAM(currentInput, setDataTable2LedgerRAM);

            // LedgerRAM to JSON
            LedgerRAM2JSONsetting setLedgerRAM2JSON = new LedgerRAM2JSONsetting();
            setLedgerRAM2JSON.rowThread = setDataTable2JSON.rowThread;            
            setLedgerRAM2JSON.tableName = setDataTable2JSON.tableName;
            LedgerRAM2JSONdataFlow process2 = new LedgerRAM2JSONdataFlow();
            StringBuilder output2 = process2.LedgerRAM2JSON(output1, setLedgerRAM2JSON);

            return output2;
        }
        public StringBuilder dataTable2XML(DataTable currentInput, dataTable2XMLsetting setDataTable2XML)
        {
            // DataTable to LedgerRAM
            dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
            setDataTable2LedgerRAM.columnThread = setDataTable2XML.columnThread;
            dataTable2LedgerRAMdataFlow process1 = new dataTable2LedgerRAMdataFlow();
            LedgerRAM output1 = process1.dataTable2LedgerRAM(currentInput, setDataTable2LedgerRAM);

            // LedgerRAM to XML
            LedgerRAM2XMLsetting setLedgerRAM2XML = new LedgerRAM2XMLsetting();           
            setLedgerRAM2XML.tableName = setDataTable2XML.tableName;
            LedgerRAM2XMLdataFlow process2 = new LedgerRAM2XMLdataFlow();
            StringBuilder output2 = process2.LedgerRAM2XML(output1, setLedgerRAM2XML);

            return output2;
        }

    }
}
