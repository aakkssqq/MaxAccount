using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class reverseCrosstabSetting
    {   
        public List<string> columnName { get; set; }
        public int columnThread = 100;        
        public string[] textTypePartialMatch = { "A/C", "Number", "Invoice", "Account", "Document"};
    }

    public class reverseCrosstab
    {
        public LedgerRAM reverseCrosstabProcess(LedgerRAM currentTable, reverseCrosstabSetting currentSetting)
        {           
            // validation of crosstab table

            bool isNumber;
            double number;
            Dictionary<int, int> eachRow = new Dictionary<int, int>();
            int firstRow = 0;
            int firstColumn = 0;
            int lastValue = 0;
            Dictionary<int, int> _firstColumn = new Dictionary<int, int>();
            List<int> excludeColumn = new List<int>();

            for (int y = 1; y < currentTable.factTable[0].Count; y++)
            {
                eachRow[y] = 0;

                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (currentTable.key2Value[x][currentTable.factTable[x][y]].ToUpper().Contains("DATE"))
                        if (!excludeColumn.Contains(x))
                            excludeColumn.Add(x);

                    foreach (var pair in currentSetting.textTypePartialMatch)
                        if (currentTable.key2Value[x][currentTable.factTable[x][y]].ToUpper().Contains(pair))
                            if(!excludeColumn.Contains(x))
                                excludeColumn.Add(x);

                    if (!excludeColumn.Contains(x))
                    {
                        isNumber = double.TryParse(currentTable.key2Value[x][currentTable.factTable[x][y]], out number);

                        if (isNumber == true)
                        {
                            eachRow[y]++;
                            if (!_firstColumn.ContainsKey(y))
                                _firstColumn.Add(y, x);
                        }
                        else
                            if (eachRow[y] > 0)
                            eachRow[y]--;
                    }
                }

                if (y > 20)
                    break;
            }

            int u = 0;
            bool isColumnTrue = true;
            bool isRowTrue = true;
            bool isValidateColumn = false;
            bool isValidateRow = false;

            foreach (var pair in _firstColumn)
            {
                if (pair.Value > 0)
                {
                    if (pair.Value == lastValue)
                    {
                        if (firstColumn == 0)
                        {
                            firstColumn = pair.Value;
                            isValidateColumn = true;
                        }

                        lastValue = pair.Value;
                    }
                    else if (isValidateColumn == true)
                        isColumnTrue = false;

                    if (u > 0)
                        lastValue = pair.Value;
                }

                u++;
            }


            lastValue = 0; u = 0;
            foreach (var pair in eachRow)
            {
                if (pair.Value > 0)
                {
                    if (pair.Value == lastValue)
                    {
                        if (firstRow == 0)
                        {
                            firstRow = pair.Key;
                            isValidateRow = true;
                        }

                        lastValue = pair.Value;
                    }

                    else if (isValidateRow == true)
                        isRowTrue = false;

                    if (u > 0)
                        lastValue = pair.Value;
                }

                u++;
            }

            LedgerRAM currentOutput = new LedgerRAM();
            LedgerRAM filterOutput = new LedgerRAM();          

            if (isColumnTrue == true && isRowTrue == true)
            {
                Dictionary<int, string> crosstabColumnName = new Dictionary<int, string>();
                Dictionary<string, int> crosstabUpperColumnName2ID = new Dictionary<string, int>();
                Dictionary<int, string> crosstabDataType = new Dictionary<int, string>();
                Dictionary<int, List<double>> crosstabFactTable = new Dictionary<int, List<double>>();
                Dictionary<int, Dictionary<double, string>> crosstabKey2Value = new Dictionary<int, Dictionary<double, string>>();
                Dictionary<int, Dictionary<string, double>> crosstabValue2Key = new Dictionary<int, Dictionary<string, double>>();
                List<string> upperMeasurement = new List<string>();
                List<string> measurement = new List<string>();

                int count = 0;
                string textA = null;
                string textB = null;
                string textC = null;

                crosstabColumnName.Add(0, "Index");
                crosstabUpperColumnName2ID.Add("INDEX", 0);
                crosstabDataType.Add(0, "Number");
                crosstabFactTable.Add(0, new List<double>());
                crosstabFactTable[0].Add(0);
                crosstabKey2Value.Add(0, new Dictionary<double, string>());
                crosstabValue2Key.Add(0, new Dictionary<string, double>());

                crosstabColumnName.Add(1, currentTable.columnName[firstColumn - 1]);
                crosstabUpperColumnName2ID.Add(currentTable.columnName[firstColumn - 1].ToUpper(), 1);
                crosstabDataType.Add(1, "Text");
                crosstabFactTable.Add(1, new List<double>());
                crosstabFactTable[1].Add(1);
                crosstabKey2Value.Add(1, new Dictionary<double, string>());
                crosstabValue2Key.Add(1, new Dictionary<string, double>());

                for (int y = 1; y < firstRow - 2; y++)
                {
                    crosstabFactTable.Add(y + 1, new List<double>());
                    crosstabFactTable[y + 1].Add(y + 1);
                    crosstabKey2Value.Add(y + 1, new Dictionary<double, string>());
                    crosstabValue2Key.Add(y + 1, new Dictionary<string, double>());

                    crosstabColumnName.Add(y + 1, currentTable.key2Value[firstColumn - 1][currentTable.factTable[firstColumn - 1][y]]);
                    crosstabUpperColumnName2ID.Add(currentTable.key2Value[firstColumn - 1][currentTable.factTable[firstColumn - 1][y]].ToUpper(), y + 1);
                    crosstabDataType.Add(y + 1, "Text");                   
                }

                crosstabColumnName.Add(firstRow - 1, "Measure");
                crosstabUpperColumnName2ID.Add("MEASURE", firstRow - 1);
                crosstabDataType.Add(firstRow - 1, "Text");

                crosstabFactTable.Add(firstRow - 1, new List<double>());
                crosstabFactTable[firstRow - 1].Add(firstRow - 1);
                crosstabKey2Value.Add(firstRow - 1, new Dictionary<double, string>());
                crosstabValue2Key.Add(firstRow - 1, new Dictionary<string, double>());

                /*
                 bool isTaskAcompleted = false;
                 bool isTaskBcompleted = false;
                 bool isTaskCcompleted = false;

                 Thread taskA = new Thread(xColumnOfColumn);
                 Thread taskB = new Thread(xColumn);
                 Thread taskC = new Thread(measurementColumn);

                 taskA.Start();
                 taskB.Start();
                 taskC.Start();

                 do
                 {
                     Thread.Sleep(2);

                 } while (isTaskAcompleted == false || isTaskBcompleted == false || isTaskCcompleted == false);
               */
                xColumnOfColumn();
                xColumn();
                measurementColumn();


                void xColumnOfColumn()
                {
                    int indexChar = 0;

                    for (int x = firstColumn; x < currentTable.columnName.Count; x++)
                    {
                        crosstabFactTable[0].Add(x);
                        indexChar = currentTable.columnName[x].IndexOf("!");

                        if (indexChar > 0)
                            textA = currentTable.columnName[x].Substring(0, indexChar);
                        else
                            textA = currentTable.columnName[x];

                        if (crosstabValue2Key[1].ContainsKey(textA))
                        {
                            crosstabFactTable[1].Add(crosstabValue2Key[1][textA]);
                        }
                        else
                        {
                            count = crosstabValue2Key[1].Count;
                            crosstabKey2Value[1].Add(count, textA);
                            crosstabValue2Key[1].Add(textA, count);
                            crosstabFactTable[1].Add(count);
                        }
                    }

                   // isTaskAcompleted = true;
                }

                // read x columns from fact table
                void xColumn()
                {
                    for (int y = 1; y < firstRow - 2; y++)
                    {  
                        for (int x = firstColumn; x < currentTable.factTable.Count; x++)
                        {
                            textB = currentTable.key2Value[x][currentTable.factTable[x][y]];

                            if (crosstabValue2Key[y + 1].ContainsKey(textB))
                                crosstabFactTable[y + 1].Add(crosstabValue2Key[y + 1][textB]);
                            else
                            {
                                count = crosstabValue2Key[y + 1].Count;
                                crosstabKey2Value[y + 1].Add(count, textB);
                                crosstabValue2Key[y + 1].Add(textB, count);
                                crosstabFactTable[y + 1].Add(count);
                            }
                        }
                    }

                    //isTaskBcompleted = true;
                }

                // read measurement columns

                void measurementColumn()
                {
                    for (int x = firstColumn; x < currentTable.factTable.Count; x++)
                    {
                        textC = currentTable.key2Value[x][currentTable.factTable[x][firstRow - 2]];

                        if (!upperMeasurement.Contains(textC.ToUpper()))
                        {
                            upperMeasurement.Add(textC.ToUpper());
                            measurement.Add(textC);
                        }

                        if (crosstabValue2Key[firstRow - 1].ContainsKey(textC.ToUpper()))
                            crosstabFactTable[firstRow - 1].Add(crosstabValue2Key[firstRow - 1][textC.ToUpper()]);
                        else
                        {
                            count = crosstabValue2Key[firstRow - 1].Count;
                            crosstabKey2Value[firstRow - 1].Add(count, textC.ToUpper());
                            crosstabValue2Key[firstRow - 1].Add(textC.ToUpper(), count);
                            crosstabFactTable[firstRow - 1].Add(count);
                        }
                    }

                    //isTaskCcompleted = true;                    
                }

                currentOutput.columnName = crosstabColumnName;
                currentOutput.upperColumnName2ID = crosstabUpperColumnName2ID;
                currentOutput.dataType = crosstabDataType;
                currentOutput.factTable = crosstabFactTable;
                currentOutput.key2Value = crosstabKey2Value;
                currentOutput.value2Key = crosstabValue2Key;

                // filter x-crosstab by different measurements

                List<string> selectedColumnName = new List<string>();
                Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
                Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();
                Dictionary<string, LedgerRAM> measurementRamStore = new Dictionary<string, LedgerRAM>();
                int xRowCount = 0;
                int yRowCount = 0;

                filter newFilter = new filter();
                filterSetting setFilter = new filterSetting();
                selectedColumnName.Add("Measure");
                compareOperator.Add(0, new List<string>());
                compareOperator[0].Add("=");
                selectedTextNumber.Add(0, new List<string>());
                setFilter.filterType = "And";
                setFilter.selectedColumnName = selectedColumnName;
                setFilter.compareOperator = compareOperator;

                foreach (var columnName in measurement)
                {
                    selectedTextNumber[0].Clear();
                    selectedTextNumber[0].Add(columnName.ToUpper());
                    setFilter.selectedTextNumber = selectedTextNumber;
                    filterOutput = newFilter.filterByOneRow(currentOutput, setFilter, null);
                    measurementRamStore.Add(columnName.ToUpper(), filterOutput);
                    xRowCount = measurementRamStore[columnName.ToUpper()].factTable[0].Count - 1;
                }

                // Write to final table

                Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
                Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
                Dictionary<int, string> resultDataType = new Dictionary<int, string>();
                Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
                Dictionary<int, List<double>> yColumnFactTable = new Dictionary<int, List<double>>();
                Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
                Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

                for (int x = 0; x < firstColumn; x++)
                {
                    resultColumnName.Add(x, currentTable.key2Value[x][currentTable.factTable[x][firstRow - 2]]);
                    resultUpperColumnName2ID.Add(currentTable.key2Value[x][currentTable.factTable[x][firstRow - 2]].ToUpper(), x);
                    resultDataType.Add(x, "Text");
                    yColumnFactTable.Add(x, new List<double>());
                    resultFactTable.Add(x, new List<double>());
                    resultFactTable[x].Add(x);                   
                }               

                for (int x = 1; x < crosstabColumnName.Count - 1; x++)
                {
                    resultColumnName.Add(x + firstColumn - 1, crosstabColumnName[x]);
                    resultUpperColumnName2ID.Add(crosstabColumnName[x].ToUpper(), x + firstColumn - 1);
                    resultDataType.Add(x + firstColumn - 1, "Text");
                    resultFactTable.Add(x + firstColumn - 1, new List<double>());
                    resultFactTable[x + firstColumn - 1].Add(x + firstColumn - 1);
                }

                int createdColumn = firstColumn + crosstabColumnName.Count - 2;

                for (int x = 0; x < measurement.Count; x++)
                {
                    resultColumnName.Add(x + createdColumn, measurement[x]);
                    resultUpperColumnName2ID.Add(measurement[x].ToUpper(), x + createdColumn);
                    resultDataType.Add(x + createdColumn, "Number");
                    resultFactTable.Add(x + createdColumn, new List<double>());
                }

                bool isTask1Completed = false;
                bool isTask2Completed = false;
               
                Thread task1 = new Thread(writeTextColumn);
                Thread task2 = new Thread(writeNumberColumn);
                task1.Start();                
                task2.Start();

                do
                {
                    Thread.Sleep(2);

                } while (isTask1Completed == false || isTask2Completed == false);

                void writeTextColumn()
                {
                    for (int x = 0; x < firstColumn; x++)
                    {
                        for (int y = firstRow - 1; y < currentTable.factTable[x].Count; y++)
                            yColumnFactTable[x].Add(currentTable.factTable[x][y]);

                        yRowCount = yColumnFactTable[x].Count;

                        for (int i = 0; i < xRowCount; i++)
                            resultFactTable[x].AddRange(yColumnFactTable[x]);

                        resultKey2Value.Add(x, currentTable.key2Value[x]);
                        resultValue2Key.Add(x, currentTable.value2Key[x]);
                    }

                    for (int x = 1; x < crosstabColumnName.Count - 1; x++)
                    {
                        for (int y = 0; y < xRowCount; y++)
                            for (int i = 0; i < yRowCount; i++)
                                resultFactTable[x + firstColumn - 1].Add(filterOutput.factTable[x][y + 1]);

                        resultKey2Value.Add(x + firstColumn - 1, filterOutput.key2Value[x]);
                        resultValue2Key.Add(x + firstColumn - 1, filterOutput.value2Key[x]);
                    }

                    isTask1Completed = true;
                }


                void writeNumberColumn()
                {
                    ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
                    ConcurrentDictionary<int, reverseCrosstab> writeColumnThread = new ConcurrentDictionary<int, reverseCrosstab>();

                    for (int worker = 0; worker < measurement.Count; worker++)
                        writeColumnThread.TryAdd(worker, new reverseCrosstab());

                    var options = new ParallelOptions()
                    {
                        MaxDegreeOfParallelism = currentSetting.columnThread
                    };

                    Parallel.For(0, measurement.Count, options, x =>
                    {
                        resultFactTable[x + createdColumn] = oneMeasureColumn(x, checkThreadCompleted, measurementRamStore, currentTable, firstRow, measurement, createdColumn, resultFactTable);
                    });

                    do
                    {
                        Thread.Sleep(2);

                    } while (checkThreadCompleted.Count < measurement.Count);

                    isTask2Completed = true;
                }


                currentOutput.columnName = resultColumnName;
                currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
                currentOutput.dataType = resultDataType;
                currentOutput.factTable = resultFactTable;
                currentOutput.key2Value = resultKey2Value;
                currentOutput.value2Key = resultValue2Key;

                for (int x = 0; x < resultColumnName.Count; x++)
                {
                  //  Console.WriteLine(firstRow + " " + resultColumnName[x] + "  " + resultDataType[x] + " " + resultFactTable[x].Count + "  " + resultFactTable[x][0]);
                }
            }

            

            return currentOutput;
        }
        public List<double> oneMeasureColumn(int x, ConcurrentQueue<int> checkThreadCompleted, Dictionary<string, LedgerRAM> measurementRamStore, LedgerRAM currentTable, int firstRow, List<string> measurement, int createdColumn, Dictionary<int, List<double>> resultFactTable)
        {
            List<double> factTable = new List<double>();

            factTable.Add(x + createdColumn);

            for (int i = 1; i < measurementRamStore[measurement[x].ToUpper()].factTable[0].Count; i++)
            {
                for (int y = firstRow - 1; y < currentTable.factTable[0].Count; y++)
                {
                    var crosstabIndexCell = Convert.ToInt32(measurementRamStore[measurement[x].ToUpper()].factTable[0][i]);
                    bool success = double.TryParse(currentTable.key2Value[crosstabIndexCell][currentTable.factTable[crosstabIndexCell][y]], out double amount);

                    if (success == true)                                         
                        factTable.Add(amount);                  
                    else
                        factTable.Add(0);                  
                }
            }

            checkThreadCompleted.Enqueue(x);
            return factTable;
        }
    }
}
