using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class csv2LedgerRAMSetting
    {
        public int rowThread = 100;
        public int columnThread = 100;
        public int separator = 44;
        public string filePath { get; set; }
        public string[] textTypeFullMatch = { "Account", "Document", };
        public string[] textTypePartialMatch = { "A/C", "Number", "Invoice", "Account", "Document"};
        public bool isOneColumn;
        public double validateByteRatio = 0.01; 
        public double validateRowDataTypeRatio = 0.01;
        public int maxValidateRowRow = 100;
        public string commonTable { get; set; }
    }

    public class internalVariable
    {
        public byte[] csvBytestream { get; set; }
        public List<int> rowSeparator { get; set; } // one or two row separator, e.g. 0D and 0A      
        public int validateRowDataType { get; set; } // number of rows for validation of data type
        public bool useDoubleQuote { get; set; } // DoubleQuote can use to skip separator e.g. "Column, Name" "Column,value"
        public List<int> bytestreamSegmentAddress { get; set; } // divide bytestream by different segment for multi-thread processing
        public Dictionary<int, List<int>> cellAddress { get; set; } // record first byte address for each column
    }

    public class csv2LedgerRAMDataFlow
    {      
        public LedgerRAM csv2LedgerRAM(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentSetting)
        {
            LedgerRAM currentOutput = new LedgerRAM();
            internalVariable currentVariable = new internalVariable();

            if(currentSetting.filePath.ToString().Contains(":\\"))
               currentVariable.csvBytestream = File.ReadAllBytes(currentSetting.filePath.ToString());
            else
               currentVariable.csvBytestream = File.ReadAllBytes("Data" + "\\" + currentSetting.filePath.ToString());

            isCSV(currentSetting, currentVariable, currentOutput); // determinate whether current bytestream is a qualified CSV file          

            // to accept csv file having more than one row (include header row)
            // to accept file having more than one column
            // to accept validated rows has same number of csv separator

            if (currentSetting.isOneColumn == true)
            {
                currentSetting.separator = 127;

                if (currentOutput.validateRow >= 1 && currentOutput.tableColumnCountExceptionList[0] == 1 && currentOutput.tableColumnCountExceptionList.Count == 1)
                {
                    findBytestreamSegmentAddress(currentSetting, currentVariable, currentOutput);
                    findCellAddressMultithread(currentSetting, currentVariable, currentOutput);
                    determineDataType(currentSetting, currentVariable, currentOutput);
                    createLedgerRAMMultithread(ramStore, currentSetting, currentVariable, currentOutput);
                }
            }
            else
            {
                if (currentOutput.validateRow > 1 && currentOutput.tableColumnCountExceptionList[0] > 1 && currentOutput.tableColumnCountExceptionList.Count == 1)
                {
                    findBytestreamSegmentAddress(currentSetting, currentVariable, currentOutput);
                    findCellAddressMultithread(currentSetting, currentVariable, currentOutput);
                    determineDataType(currentSetting, currentVariable, currentOutput);
                    createLedgerRAMMultithread(ramStore, currentSetting, currentVariable, currentOutput);
                }
            }

            return currentOutput;
        }
        public void isCSV(csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        {
            int n = 0;
            int row = 0;
            int openCloseDoubleQuote = 1;
            int tableColumnCount;
            int separator;
            double validateByteRatio;
            Dictionary<int, int> tableColumnCountExceptionList = new Dictionary<int, int>();
            List<int> rowSeparator = new List<int>();
            currentOutput.fileByteLength = currentVariable.csvBytestream.Length;         

            do
            {
                tableColumnCount = 1;

                do // determine number of table column
                {
                    if (currentVariable.csvBytestream[n] == 34) // doubleQuote char
                    {
                        currentVariable.useDoubleQuote = true;
                        openCloseDoubleQuote = openCloseDoubleQuote * -1;
                    }

                    if (openCloseDoubleQuote == 1)
                        separator = currentSetting.separator;
                    else // detect open double quote
                        separator = 127; // delete key, ignore current sepearator    

                    if (currentVariable.csvBytestream[n] == separator)
                        tableColumnCount++;

                    n++;

                    if (rowSeparator.Count == 0)
                    {
                        if (currentVariable.csvBytestream[n] == 13)
                            rowSeparator.Add(13);

                        if (currentVariable.csvBytestream[n] == 10)
                            rowSeparator.Add(10);
                    }

                } while (!(currentVariable.csvBytestream[n] == 13 || currentVariable.csvBytestream[n] == 10) && n < currentOutput.fileByteLength - 2); // check first row only to determine number of column               

                n++;

                if (row == 0 || (row > 0 && tableColumnCountExceptionList[0] != tableColumnCount))
                {                  
                    tableColumnCountExceptionList.Add(row, tableColumnCount);
                }

                if (rowSeparator.Count == 1)
                {
                    if (currentVariable.csvBytestream[n] == 13)
                        rowSeparator.Add(13);

                    if (currentVariable.csvBytestream[n] == 10)
                        rowSeparator.Add(10);
                }

                row++;              

                if (row < currentSetting.maxValidateRowRow)
                    validateByteRatio = 1;
                else
                    validateByteRatio = currentSetting.validateByteRatio;


            } while (n < (currentOutput.fileByteLength - 1) * validateByteRatio);

            currentOutput.tableColumnCountExceptionList = tableColumnCountExceptionList;
            currentVariable.rowSeparator = rowSeparator;
            currentOutput.validateRow = row;
        }
        public void findBytestreamSegmentAddress(csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        {            
            List<int> bytestreamRowSegmentAddress = new List<int>();
            byte[] csvBytestream = currentVariable.csvBytestream;
            int fileByteLength = currentOutput.fileByteLength;

            if (csvBytestream[fileByteLength - 1] != 13 && csvBytestream[fileByteLength - 1] != 10) // check exist of 10 and/or 13 at the end of file
            {
                fileByteLength = fileByteLength + 1;
                Array.Resize(ref csvBytestream, fileByteLength);
                csvBytestream[csvBytestream.GetUpperBound(0)] = 13;
                fileByteLength = fileByteLength + 1;
                Array.Resize(ref csvBytestream, fileByteLength);
                csvBytestream[csvBytestream.GetUpperBound(0)] = 10;
            }           

            int maxThread = Convert.ToInt32(Math.Round((double)(fileByteLength / 1000000), 0));


            if (maxThread < 1)
                maxThread = 1;

            if (currentSetting.rowThread > maxThread)
                currentSetting.rowThread = maxThread;
            
            int rowSegmentLength = Convert.ToInt32(Math.Round((double)(fileByteLength / currentSetting.rowThread), 0));           

            bytestreamRowSegmentAddress.Add(0);
            int nextByte = -1;

            if (fileByteLength > 10000 && currentSetting.rowThread > 1)
            {
                for (int i = 1; i < currentSetting.rowThread; i++)
                {
                    do
                    {
                        nextByte++;
                    } while (csvBytestream[rowSegmentLength * i + nextByte] != currentVariable.rowSeparator[currentVariable.rowSeparator.Count - 1]);

                    bytestreamRowSegmentAddress.Add(rowSegmentLength * i + nextByte + 1);

                    if (i == currentSetting.rowThread - 1 && rowSegmentLength * i + nextByte + 1 < fileByteLength)
                        bytestreamRowSegmentAddress.Add(fileByteLength);
                }
            }
            else
                 bytestreamRowSegmentAddress.Add(fileByteLength);

            currentOutput.fileByteLength = fileByteLength;
            currentVariable.csvBytestream = csvBytestream;
            currentVariable.bytestreamSegmentAddress = bytestreamRowSegmentAddress;
        }
        public void findCellAddressMultithread(csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        {
            Dictionary<int, List<int>> cellAddress = new Dictionary<int, List<int>>();

            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();

            ConcurrentDictionary<int, Dictionary<int, List<int>>> cellAddressForOneSegment = new ConcurrentDictionary<int, Dictionary<int, List<int>>>();

            ConcurrentDictionary<int, csv2LedgerRAMDataFlow> concurrentSegmentAddress = new ConcurrentDictionary<int, csv2LedgerRAMDataFlow>();

            for (int worker = 0; worker < currentVariable.bytestreamSegmentAddress.Count - 1; worker++) concurrentSegmentAddress.TryAdd(worker, new csv2LedgerRAMDataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };         

            Parallel.For(0, currentVariable.bytestreamSegmentAddress.Count - 1, options, currentSegment =>
            {               
                cellAddressForOneSegment[currentSegment] = concurrentSegmentAddress[currentSegment].findCellAddress(currentSegment, checkSegmentThreadCompleted, currentSetting, currentVariable, currentOutput);
            });           

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < currentVariable.bytestreamSegmentAddress.Count - 1);           

            cellAddress.Clear();
            for (int i = 0; i <= currentOutput.tableColumnCountExceptionList[0]; i++)
                cellAddress.Add(i, new List<int>());

            for (int currentSegment = 0; currentSegment < currentVariable.bytestreamSegmentAddress.Count - 1; currentSegment++)
                for (int i = 0; i <= currentOutput.tableColumnCountExceptionList[0]; i++)
                    cellAddress[i].AddRange(cellAddressForOneSegment[currentSegment][i]);

            if(cellAddress.Count > 1)
                if(cellAddress[0].Count == cellAddress[1].Count + 1)               
                    cellAddress[0].RemoveAt(cellAddress[0].Count - 1);

            currentVariable.cellAddress = cellAddress;
        }
        public Dictionary<int, List<int>> findCellAddress(int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        { 
            Dictionary<int, List<int>> cellAddressForOneSegment = new Dictionary<int, List<int>>();
            int maxColumn = currentOutput.tableColumnCountExceptionList[0];
            int nextRowChar = currentVariable.rowSeparator.Count;
            int separator;

            int column = 1;

            for (int i = 0; i <= maxColumn; i++)
                cellAddressForOneSegment.Add(i, new List<int>());

            if (currentSegment == 0)
                cellAddressForOneSegment[0].Add(0);

            var fromAddress = currentVariable.bytestreamSegmentAddress[currentSegment];
            var toAddress = currentVariable.bytestreamSegmentAddress[currentSegment + 1];                       

            if (currentVariable.useDoubleQuote == false)
            {
                for (int i = fromAddress; i < toAddress; i++)
                {
                    
                    if (column >= maxColumn)
                    {
                        if (currentVariable.csvBytestream[i] == currentVariable.rowSeparator[0])
                        {
                            cellAddressForOneSegment[maxColumn].Add(i + nextRowChar);                            
                            
                            cellAddressForOneSegment[0].Add(i + nextRowChar);  //????

                            column = 1;
                        }
                        else if (currentVariable.csvBytestream[i] == currentSetting.separator)
                        {
                            currentVariable.useDoubleQuote = true; // unmatch column
                            for (int x = 0; x <= maxColumn; x++)
                                cellAddressForOneSegment[x].Clear();

                            if (currentSegment == 0)
                                cellAddressForOneSegment[0].Add(0);
                            break;
                        }
                    }

                    if (currentVariable.csvBytestream[i] == currentSetting.separator)
                    {
                        if (column < maxColumn)
                        {
                            cellAddressForOneSegment[column].Add(i + 1);                            
                            column++;
                        }
                    }
                }
            }           
            
            if (currentVariable.useDoubleQuote == true) // suspect existence of double quote to hide ","
            {               
                column = 1;
                int openCloseDoubleQuote = 1;
                for (int i = fromAddress; i < toAddress; i++)
                {                   
                    if (currentVariable.csvBytestream[i] == 34) // double quote
                    {
                        openCloseDoubleQuote = openCloseDoubleQuote * -1;
                        currentVariable.csvBytestream[i] = 32; // replace double quote by space
                    }

                    if (openCloseDoubleQuote == 1)
                        separator = currentSetting.separator; 
                    else
                        separator = 127; // del key

                    if (column >= maxColumn)
                    {
                        if (currentVariable.csvBytestream[i] == currentVariable.rowSeparator[0])
                        {
                            cellAddressForOneSegment[maxColumn].Add(i + nextRowChar);
                            cellAddressForOneSegment[0].Add(i + nextRowChar);
                            column = 1;
                        }
                    }

                    if (currentVariable.csvBytestream[i] == separator && column < maxColumn)
                    {
                        cellAddressForOneSegment[column].Add(i + 1);
                        column++;
                    }
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);            
            return cellAddressForOneSegment;
        }    
        public void determineDataType(csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        {
            StringBuilder cellValue = new StringBuilder();           
            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            string currentcellValue;
            bool isNumber;
            int stringCount;
            int validateRowDataType = 10;
            int expectedValidateRow = (int)Math.Round(currentSetting.validateRowDataTypeRatio * currentVariable.cellAddress[0].Count, 0);

            Dictionary<string, int> textTypeFullMatch = new Dictionary<string, int>();

            for (int i = 0; i < currentSetting.textTypeFullMatch.Length; i++)           
                textTypeFullMatch.Add(currentSetting.textTypeFullMatch[i].ToUpper(), i);

            Dictionary<string, int> textTypePartialMatch = new Dictionary<string, int>();

            for (int i = 0; i < currentSetting.textTypePartialMatch.Length; i++)
                textTypePartialMatch.Add(currentSetting.textTypePartialMatch[i].ToUpper(), i);

            if (expectedValidateRow > validateRowDataType)
                validateRowDataType = expectedValidateRow;
            else
            {
                if(currentVariable.cellAddress[0].Count > 10)
                   validateRowDataType = 10;
                else
                   validateRowDataType = currentVariable.cellAddress[0].Count;
            }            

            for (int x = 0; x < currentOutput.tableColumnCountExceptionList[0]; x++)
            {
                stringCount = 0;
                for (int y = 1; y < validateRowDataType; y++)
                {
                    cellValue.Clear();

                    for (int j = currentVariable.cellAddress[x][y]; j < currentVariable.cellAddress[x + 1][y] - 1; j++)
                        cellValue.Append((char)currentVariable.csvBytestream[j]);

                    currentcellValue = cellValue.ToString().Trim();                    

                    isNumber = double.TryParse(currentcellValue, out double number);

                    if (currentcellValue.Length == 0)
                        isNumber = true;

                    if (isNumber == false)
                        stringCount++;
                }
                if (stringCount > 0)
                    dataType[x] = "Text";
                else
                    dataType[x] = "Number";            
            }

            int suffix = 2;

            for (int x = 0; x < currentOutput.tableColumnCountExceptionList[0]; x++)
            {   
                cellValue.Clear();              

                for (int j = currentVariable.cellAddress[x][0]; j < currentVariable.cellAddress[x + 1][0] - 1; j++)
                    cellValue.Append((char)currentVariable.csvBytestream[j]);

                currentcellValue = cellValue.ToString().Trim();

                if (currentcellValue.Length == 0)
                {
                    columnName[x] = "Column" + x;
                    upperColumnName2ID["COLUMN" + x] = x;
                }
                else
                {

                    if (!upperColumnName2ID.ContainsKey(currentcellValue.Trim()) && !upperColumnName2ID.ContainsKey(currentcellValue.Trim().ToUpper()))
                    {
                        columnName[x] = currentcellValue.Trim();
                        upperColumnName2ID[currentcellValue.Trim().ToUpper()] = x;
                    }
                    else
                    {
                        currentcellValue = currentcellValue + "!" + suffix;
                        columnName[x] = currentcellValue.Trim();
                        upperColumnName2ID[currentcellValue.Trim().ToUpper()] = x;
                        suffix++;
                    }
                }
            }           

            for (int x = 0; x < dataType.Count; x++)
            {
                if (dataType[x] == "Number")
                {
                    if (textTypeFullMatch.ContainsKey(columnName[x].ToUpper()))
                        dataType[x] = "Text";                   

                    foreach (var pair in textTypePartialMatch)
                    {
                        if (columnName[x].ToUpper().Contains(pair.Key))
                            dataType[x] = "Text";
                    }                   

                    if (columnName[x].ToUpper().Contains("DATE"))
                        dataType[x] = "Date";
                }
                else
                {
                    if (columnName[x].ToUpper().Contains("DATE"))
                        dataType[x] = "Date";
                }

            }
         
            currentOutput.dataType = dataType;
            currentOutput.columnName = columnName;
            currentOutput.upperColumnName2ID = upperColumnName2ID;
            currentVariable.validateRowDataType = validateRowDataType;
        }        
        public void createLedgerRAMMultithread(Dictionary<string, LedgerRAM> ramStore, csv2LedgerRAMSetting currentSetting, internalVariable currentVariable, LedgerRAM currentOutput)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            
            for (int i = 0; i < currentOutput.dataType.Count; i++)
            {                
                factTable.Add(i, new List<double>());

                if (currentOutput.dataType[i] != "Number")
                {                 
                    key2Value.Add(i, new Dictionary<double, string>());
                    value2Key.Add(i, new Dictionary<string, double>());                   
                }
            }                      

            ConcurrentDictionary<int, csv2LedgerRAMDataFlow> writeColumnThread = new ConcurrentDictionary<int, csv2LedgerRAMDataFlow>();

            for (int worker = 0; worker < currentOutput.dataType.Count; worker++)
                writeColumnThread.TryAdd(worker, new csv2LedgerRAMDataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };            

            Parallel.For(0, currentOutput.dataType.Count, options, x =>
            {
                if (currentOutput.dataType[x] == "Number")
                {
                    factTable[x] = createLedgerRAMMeasure(x, checkThreadCompleted, currentVariable, currentOutput);
                    
                    if(factTable[x].Count == 0)
                        (factTable[x], key2Value[x], value2Key[x]) = createLedgerRAMDimensionKey(ramStore, x, checkThreadCompleted, currentVariable, currentOutput, currentSetting);                   
                }
                else
                    (factTable[x], key2Value[x], value2Key[x]) = createLedgerRAMDimensionKey(ramStore, x, checkThreadCompleted, currentVariable, currentOutput, currentSetting);
            });

            do
            {
                Thread.Sleep(2);              

            } while (checkThreadCompleted.Count < currentOutput.dataType.Count);

            currentOutput.factTable = new Dictionary<int, List<double>>(factTable);
            currentOutput.key2Value = new Dictionary<int, Dictionary<double, string>>(key2Value);
            currentOutput.value2Key = new Dictionary<int, Dictionary<string, double>>(value2Key);

            if (currentSetting.commonTable != null)
            {
                ramStore[currentSetting.commonTable].key2Value = new Dictionary<int, Dictionary<double, string>>(key2Value);
                ramStore[currentSetting.commonTable].value2Key = new Dictionary<int, Dictionary<string, double>>(value2Key);
            }

        }
        public (List<double> factTable,Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) createLedgerRAMDimensionKey(Dictionary<string, LedgerRAM> ramStore, int columnID, ConcurrentQueue<int> checkThreadCompleted, internalVariable currentVariable, LedgerRAM currentOutput, csv2LedgerRAMSetting currentSetting)
        {
          
            List<double> factTable = new List<double>();   
            StringBuilder cellValue = new StringBuilder();
            factTable.Add(columnID); // first record is column id              
            double count;

            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            if (currentSetting.commonTable != null)
            {
                if (ramStore[currentSetting.commonTable].key2Value.ContainsKey(columnID))
                {
                    key2Value = ramStore[currentSetting.commonTable].key2Value[columnID];
                    value2Key = ramStore[currentSetting.commonTable].value2Key[columnID];
                }
            }           


            for (int y = 1; y < currentVariable.cellAddress[columnID].Count; y++)
            {
                cellValue.Clear();
                for (int j = currentVariable.cellAddress[columnID][y]; j < currentVariable.cellAddress[columnID + 1][y] - 1; j++)
                    cellValue.Append((char)currentVariable.csvBytestream[j]);

                string text = cellValue.ToString().Trim();

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
        public List<double> createLedgerRAMMeasure(int columnID, ConcurrentQueue<int> checkThreadCompleted, internalVariable currentVariable, LedgerRAM currentOutput)
        {
            List<double> factTable = new List<double>();           
            StringBuilder cellValue = new StringBuilder();
            bool isNumber;
            factTable.Add(columnID); // first record is column id    

            for (int y = 1; y < currentVariable.cellAddress[columnID].Count; y++)
            {
                cellValue.Clear();
                for (int j = currentVariable.cellAddress[columnID][y]; j < currentVariable.cellAddress[columnID + 1][y] - 1; j++)
                    cellValue.Append((char)currentVariable.csvBytestream[j]);

                string text = cellValue.ToString().Trim();

                isNumber = double.TryParse(text, out double number);

                if (text.Length > 0)
                {
                    if (isNumber == true)
                        factTable.Add(number);
                    else
                    {
                        factTable.Clear();
                        currentOutput.dataType[columnID] = "Text";                      
                        break;
                    }
                }  
                else
                   factTable.Add(0);
            }
            checkThreadCompleted.Enqueue(columnID);
            return factTable;
        }
    }
}
