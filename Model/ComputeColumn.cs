using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class computeColumnSetting
    {
        public int rowThread = 100;
        public string calc { get; set; }
        public List<string> refColumnName { get; set; }
        public List<int> refColumnID { get; set; }
        public List<double> refConstant { get; set; }
        public bool isFirstContant { get; set; }
        public string resultColumnName { get; set; }
        public int decimalPlace = 999;
    }

    public class computeColumn
    {
        public LedgerRAM calc(LedgerRAM currentTable, computeColumnSetting currentSetting)
        {
            List<string> refColumnName = new List<string>();

            for (int x = 0; x < currentSetting.refColumnName.Count; x++)
                refColumnName.Add(currentSetting.refColumnName[x].Replace("\"", ""));

            currentSetting.refColumnName = refColumnName;           
           
            ConcurrentDictionary<int, List<double>> factTableMultithread = new ConcurrentDictionary<int, List<double>>();          
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();            
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            LedgerRAM currentOutput = new LedgerRAM();

            List<double> factTable = new List<double>();
            List<int> rowSegment = new List<int>();   
            List<int> refColumnID = new List<int>();
            List<double> refConstant = new List<double>();
            
            currentSetting.isFirstContant = false;
            bool isNumber;
            int sameColumnID = -1;

            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                if (currentSetting.refColumnName.Contains(currentTable.columnName[i]))
                {
                    refColumnID.Add(i);

                    if (currentTable.columnName[i].ToUpper() == currentSetting.resultColumnName.ToUpper())
                        sameColumnID = i;
                }
            }          

            for (int i = 0; i < currentSetting.refColumnName.Count; i++)
            {
                if (!currentTable.columnName.ContainsValue(currentSetting.refColumnName[i]))
                {
                    isNumber = double.TryParse(currentSetting.refColumnName[i], out double number);                  

                    if (isNumber == true)
                    {
                        if (i == 0)
                            currentSetting.isFirstContant = true;                       

                        refConstant.Add(number);
                    }
                }
            }

            currentSetting.refColumnID = refColumnID;
            currentSetting.refConstant = refConstant;          

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

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new LedgerRAM2CSVdataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };
            
            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                if (currentSetting.calc.ToUpper() == "ADD")
                    factTableMultithread[currentSegment] = add(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting);

                if (currentSetting.calc.ToUpper() == "SUBTRACT")
                    factTableMultithread[currentSegment] = subtract(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting);

                if (currentSetting.calc.ToUpper() == "MULTIPLY")
                    factTableMultithread[currentSegment] = multiply(rowSegment, currentSegment, checkSegmentThreadCompleted,currentTable, currentSetting);

                if (currentSetting.calc.ToUpper() == "DIVIDE")
                    factTableMultithread[currentSegment] = divide(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting);
            });

            do
            {
                Thread.Sleep(5);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
           
            var resultColumnID = currentTable.columnName.Count;
            factTable.Add(resultColumnID);
            for (int i = 0; i < rowSegment.Count - 1; i++)
                factTable.AddRange(factTableMultithread[i]);           
           

            int u = 0;

            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                if (i == sameColumnID)
                    u++;

                if (i != sameColumnID)
                {                    
                    resultDataType.Add(i - u, currentTable.dataType[i]);
                    resultColumnName.Add(i - u, currentTable.columnName[i]);
                    resultUpperColumnName2ID.Add(currentTable.columnName[i].Trim().ToUpper(), i - u);
                    resultFactTable.Add(i - u, currentTable.factTable[i]);

                    if (currentTable.dataType[i] != "Number")
                    {
                        resultKey2Value.Add(i - u, currentTable.key2Value[i]);
                        resultValue2Key.Add(i - u, currentTable.value2Key[i]);
                    }
                }               
            }

            resultDataType.Add(resultColumnID - u, "Number");
            resultColumnName.Add(resultColumnID - u, currentSetting.resultColumnName);
            resultUpperColumnName2ID.Add(currentSetting.resultColumnName.ToUpper(), resultColumnID - u);
            resultFactTable.Add(resultColumnID - u, factTable);

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;          

            return currentOutput;
        }

        public List<double> add(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, computeColumnSetting currentSetting)
        {
            List<double> factTable = new List<double>();
            double currentNumber;
         
            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                currentNumber = 0;

                for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                    currentNumber = currentNumber + currentTable.factTable[currentSetting.refColumnID[x]][y];
                 
                for (int x = 0; x < currentSetting.refConstant.Count; x++)
                    currentNumber = currentNumber + currentSetting.refConstant[x];

                if (currentSetting.decimalPlace == 999)
                    factTable.Add(currentNumber);
                else
                    factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }

        public List<double> subtract(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, computeColumnSetting currentSetting)
        {
            List<double> factTable = new List<double>();
            double currentNumber;
            
            if (currentSetting.isFirstContant == true)
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = currentSetting.refConstant[0];

                    for (int x = 1; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber - currentSetting.refConstant[x];

                    for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber - currentTable.factTable[currentSetting.refColumnID[x]][y];

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            else
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = currentTable.factTable[currentSetting.refColumnID[0]][y];

                    for (int x = 1; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber - currentTable.factTable[currentSetting.refColumnID[x]][y];

                    for (int x = 0; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber - currentSetting.refConstant[x];

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }

        public List<double> multiply(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, computeColumnSetting currentSetting)
        {
            List<double> factTable = new List<double>();
            double currentNumber;

            if (currentSetting.isFirstContant == true)
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = 1;

                    for (int x = 0; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber * currentSetting.refConstant[x];

                    for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber * currentTable.factTable[currentSetting.refColumnID[x]][y];                    

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            else
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = 1;

                    for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber * currentTable.factTable[currentSetting.refColumnID[x]][y];

                    for (int x = 0; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber * currentSetting.refConstant[x];

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }

        public List<double> divide(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, computeColumnSetting currentSetting)
        {
            List<double> factTable = new List<double>();
            double currentNumber;

            if (currentSetting.isFirstContant == true)
            { 
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = currentSetting.refConstant[0];

                    for (int x = 1; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber / currentSetting.refConstant[x];

                    for (int x = 0; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber / currentTable.factTable[currentSetting.refColumnID[x]][y];                  

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            else
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    currentNumber = currentTable.factTable[currentSetting.refColumnID[0]][y];

                    for (int x = 1; x < currentSetting.refColumnID.Count; x++)
                        currentNumber = currentNumber / currentTable.factTable[currentSetting.refColumnID[x]][y];

                    for (int x = 0; x < currentSetting.refConstant.Count; x++)
                        currentNumber = currentNumber / currentSetting.refConstant[x];

                    if (currentSetting.decimalPlace == 999)
                        factTable.Add(currentNumber);
                    else
                        factTable.Add(Math.Round((double)(currentNumber), currentSetting.decimalPlace));
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
    }
}
