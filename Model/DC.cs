using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class DCsetting
    {
        public int rowThread = 100;
        public List<string> DC2PositiveNegative { get; set; }
        public List<string> DC2NegativePositive { get; set; }
        public List<string> PositiveNegative2DC { get; set; }
        public List<string> NegativePositive2DC { get; set; }
        public bool ReverseDC { get; set; }
    }    

    public class DC
    {
        public LedgerRAM DC2Number(LedgerRAM currentTable, DCsetting currentSetting)
        {           
            List<int> DC2PositiveNegativeColumnID = new List<int>();
            List<int> DC2NegativePositiveColumnID = new List<int>();
            Dictionary<double, int> DCkey2Factor = new Dictionary<double, int>();            

            int DCcolumnID = 0;
            
            if (currentSetting.DC2PositiveNegative != null)
            {
                for (int x = 0; x < currentSetting.DC2PositiveNegative.Count; x++)
                {                  
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.DC2PositiveNegative[x].ToUpper()))
                        DC2PositiveNegativeColumnID.Add(currentTable.upperColumnName2ID[currentSetting.DC2PositiveNegative[x].ToUpper()]);
                }
            }

            if (currentSetting.DC2NegativePositive != null)
            {
                for (int x = 0; x < currentSetting.DC2NegativePositive.Count; x++)
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.DC2NegativePositive[x].ToUpper()))
                        DC2NegativePositiveColumnID.Add(currentTable.upperColumnName2ID[currentSetting.DC2NegativePositive[x].ToUpper()]);
            }

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {               
                if (currentTable.columnName[x] == "D/C")
                {
                    DCcolumnID = x;                    

                    if (currentSetting.DC2PositiveNegative != null)
                    {                       
                        DCkey2Factor.Add(currentTable.value2Key[DCcolumnID]["D"], 1);
                        DCkey2Factor.Add(currentTable.value2Key[DCcolumnID]["C"], -1);
                    }
                    else if (currentSetting.DC2NegativePositive != null)
                    {                      
                        DCkey2Factor.Add(currentTable.value2Key[DCcolumnID]["D"], -1);
                        DCkey2Factor.Add(currentTable.value2Key[DCcolumnID]["C"], 1);
                    }
                }
            }

            ConcurrentDictionary<int, Dictionary<int, List<double>>> factTableMultithread = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
           
            List<int> rowSegment = new List<int>();          
           
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
                if (currentSetting.DC2PositiveNegative != null)
                    factTableMultithread[currentSegment] = DC2PositiveNegativeSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, DCkey2Factor, DC2PositiveNegativeColumnID, DCcolumnID);

                if (currentSetting.DC2NegativePositive != null)
                    factTableMultithread[currentSegment] = DC2NegativePositiveSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, DCkey2Factor, DC2NegativePositiveColumnID, DCcolumnID);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
        
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            LedgerRAM currentOutput = new LedgerRAM();
            
            if (currentSetting.DC2PositiveNegative != null)
            {
                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (!DC2PositiveNegativeColumnID.Contains(x) && x != DCcolumnID)
                        resultFactTable[x] = currentTable.factTable[x];
                }

                for (int x = 0; x < DC2PositiveNegativeColumnID.Count; x++)
                {
                    resultFactTable.Add(DC2PositiveNegativeColumnID[x], new List<double>());
                    resultFactTable[DC2PositiveNegativeColumnID[x]].Add(DC2PositiveNegativeColumnID[x]);                  
                }
                resultFactTable.Add(DCcolumnID, new List<double>());
                resultFactTable[DCcolumnID].Add(DCcolumnID);

                for (int i = 0; i < rowSegment.Count - 1; i++)
                {
                    for (int x = 0; x < DC2PositiveNegativeColumnID.Count; x++)
                    {
                        resultFactTable[DC2PositiveNegativeColumnID[x]].AddRange(factTableMultithread[i][DC2PositiveNegativeColumnID[x]]);
                        resultFactTable[DCcolumnID].AddRange(factTableMultithread[i][DCcolumnID]);
                    }
                }
            }

            if (currentSetting.DC2NegativePositive != null)
            {
                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (!DC2NegativePositiveColumnID.Contains(x) && x != DCcolumnID)
                        resultFactTable[x] = currentTable.factTable[x];
                }

                for (int x = 0; x < DC2NegativePositiveColumnID.Count; x++)
                {
                    resultFactTable.Add(DC2NegativePositiveColumnID[x], new List<double>());
                    resultFactTable[DC2NegativePositiveColumnID[x]].Add(DC2NegativePositiveColumnID[x]);                   
                }
                resultFactTable.Add(DCcolumnID, new List<double>());
                resultFactTable[DCcolumnID].Add(DCcolumnID);

                for (int i = 0; i < rowSegment.Count - 1; i++)
                {
                    for (int x = 0; x < DC2NegativePositiveColumnID.Count; x++)
                    {
                        resultFactTable[DC2NegativePositiveColumnID[x]].AddRange(factTableMultithread[i][DC2NegativePositiveColumnID[x]]);
                        resultFactTable[DCcolumnID].AddRange(factTableMultithread[i][DCcolumnID]);
                    }
                }
            }

            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            List<string> selectColumn = new List<string>();

            for (int x = 0; x < currentTable.columnName.Count; x++)           
                if (currentTable.columnName[x] != "D/C")
                    selectColumn.Add(currentTable.columnName[x]);

            selectColumn newSelectColumn = new selectColumn();
            selectColumnSetting setSelectColumn = new selectColumnSetting();
            setSelectColumn.selectColumn = selectColumn;
            currentOutput = newSelectColumn.selectColumnName(currentOutput, setSelectColumn);

            return currentOutput;            
        }
        public Dictionary<int, List<double>> DC2PositiveNegativeSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, Dictionary<double, int> DCkey2Factor, List<int> DC2PositiveNegativeColumnID, int DCcolumnID)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < DC2PositiveNegativeColumnID.Count; x++)            
                factTable.Add(DC2PositiveNegativeColumnID[x], new List<double>());              
           
            factTable.Add(DCcolumnID, new List<double>());

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < DC2PositiveNegativeColumnID.Count; x++)
                {
                    var currentKey = currentTable.factTable[DC2PositiveNegativeColumnID[x]][y];
                    var currentDC = currentTable.factTable[DCcolumnID][y];                    
                  
                    factTable[DC2PositiveNegativeColumnID[x]].Add(currentKey * DCkey2Factor[currentDC]);
                    factTable[DCcolumnID].Add(currentTable.value2Key[DCcolumnID]["D"]);
                }

            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
        public Dictionary<int, List<double>> DC2NegativePositiveSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, Dictionary<double, int> DCkey2Factor, List<int> DC2NegativePositiveColumnID, int DCcolumnID)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < DC2NegativePositiveColumnID.Count; x++)           
                factTable.Add(DC2NegativePositiveColumnID[x], new List<double>());

            factTable.Add(DCcolumnID, new List<double>());

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < DC2NegativePositiveColumnID.Count; x++)
                {
                    var currentKey = currentTable.factTable[DC2NegativePositiveColumnID[x]][y];
                    var currentDC = currentTable.factTable[DCcolumnID][y];

                    factTable[DC2NegativePositiveColumnID[x]].Add(currentKey * DCkey2Factor[currentDC]);
                    factTable[DCcolumnID].Add(currentTable.value2Key[DCcolumnID]["C"]);
                }

            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
        public LedgerRAM number2DC(LedgerRAM currentTable, DCsetting currentSetting)
        {          
            List<int> PositiveNegative2DCColumnID = new List<int>();
            List<int> NegativePositive2DCColumnID = new List<int>();
            Dictionary<double, int> DCkey2Factor = new Dictionary<double, int>();

            int DCcolumnID = currentTable.factTable.Count;

            if (currentSetting.PositiveNegative2DC != null)
            {
                for (int x = 0; x < currentSetting.PositiveNegative2DC.Count; x++)
                {
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.PositiveNegative2DC[x].ToUpper()))
                        PositiveNegative2DCColumnID.Add(currentTable.upperColumnName2ID[currentSetting.PositiveNegative2DC[x].ToUpper()]);
                }
            }

            if (currentSetting.NegativePositive2DC != null)
            {
                for (int x = 0; x < currentSetting.NegativePositive2DC.Count; x++)
                {
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.NegativePositive2DC[x].ToUpper()))
                        NegativePositive2DCColumnID.Add(currentTable.upperColumnName2ID[currentSetting.NegativePositive2DC[x].ToUpper()]);
                }
            }

            ConcurrentDictionary<int, Dictionary<int, List<double>>> factTableMultithread = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();

            List<int> rowSegment = new List<int>();

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
                if (currentSetting.PositiveNegative2DC != null)
                    factTableMultithread[currentSegment] = PositiveNegative2DCSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, PositiveNegative2DCColumnID, DCcolumnID);

                if (currentSetting.NegativePositive2DC != null)
                    factTableMultithread[currentSegment] = NegativePositive2DCSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, NegativePositive2DCColumnID, DCcolumnID);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            LedgerRAM currentOutput = new LedgerRAM();

            if (currentSetting.PositiveNegative2DC != null)
            {
                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (!PositiveNegative2DCColumnID.Contains(x))
                        resultFactTable[x] = currentTable.factTable[x];
                }

                for (int x = 0; x < PositiveNegative2DCColumnID.Count; x++)
                {
                    resultFactTable.Add(PositiveNegative2DCColumnID[x], new List<double>());
                    resultFactTable[PositiveNegative2DCColumnID[x]].Add(PositiveNegative2DCColumnID[x]);
                }

                
                resultFactTable.Add(DCcolumnID, new List<double>());
                resultFactTable[DCcolumnID].Add(DCcolumnID);               

                for (int i = 0; i < rowSegment.Count - 1; i++)
                {
                    for (int x = 0; x < PositiveNegative2DCColumnID.Count; x++)
                    {
                        resultFactTable[PositiveNegative2DCColumnID[x]].AddRange(factTableMultithread[i][PositiveNegative2DCColumnID[x]]);
                        resultFactTable[DCcolumnID].AddRange(factTableMultithread[i][DCcolumnID]);
                    }
                }
            }

            if (currentSetting.NegativePositive2DC != null)
            {
                for (int x = 0; x < currentTable.factTable.Count; x++)
                {
                    if (!NegativePositive2DCColumnID.Contains(x))
                        resultFactTable[x] = currentTable.factTable[x];
                }

                for (int x = 0; x < NegativePositive2DCColumnID.Count; x++)
                {
                    resultFactTable.Add(NegativePositive2DCColumnID[x], new List<double>());
                    resultFactTable[NegativePositive2DCColumnID[x]].Add(NegativePositive2DCColumnID[x]);
                }
                resultFactTable.Add(DCcolumnID, new List<double>());
                resultFactTable[DCcolumnID].Add(DCcolumnID);

                for (int i = 0; i < rowSegment.Count - 1; i++)
                {
                    for (int x = 0; x < NegativePositive2DCColumnID.Count; x++)
                    {
                        resultFactTable[NegativePositive2DCColumnID[x]].AddRange(factTableMultithread[i][NegativePositive2DCColumnID[x]]);
                        resultFactTable[DCcolumnID].AddRange(factTableMultithread[i][DCcolumnID]);
                    }
                }
            }

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();            
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            // upperColumnName2ID.Clear();

            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();

            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                resultDataType.Add(i, currentTable.dataType[i]);
                resultColumnName.Add(i, currentTable.columnName[i].Trim());
                resultUpperColumnName2ID.Add(currentTable.columnName[i].Trim().ToUpper(), i);             

                if (currentTable.dataType[i] != "Number")
                {
                    resultKey2Value.Add(i, currentTable.key2Value[i]);
                    resultValue2Key.Add(i, currentTable.value2Key[i]);
                }
            }

            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            key2Value.Add(0, "D"); value2Key.Add("D", 0);
            key2Value.Add(1, "C"); value2Key.Add("C", 1);
            key2Value.Add(2, "U"); value2Key.Add("U", 2);

            resultDataType.Add(DCcolumnID, "Text");
            resultColumnName.Add(DCcolumnID, "D/C");
            resultUpperColumnName2ID.Add("D/C", DCcolumnID);            
            resultKey2Value.Add(DCcolumnID, key2Value);
            resultValue2Key.Add(DCcolumnID, value2Key);

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }
        public Dictionary<int, List<double>> PositiveNegative2DCSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, List<int> PositiveNegative2DCColumnID, int DCcolumnID)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < PositiveNegative2DCColumnID.Count; x++)
                factTable.Add(PositiveNegative2DCColumnID[x], new List<double>());

            factTable.Add(DCcolumnID, new List<double>());
            
            bool isPositive;
            bool isNegative;           

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                isPositive = true;
                isNegative = true;

                for (int x = 0; x < PositiveNegative2DCColumnID.Count; x++)
                {
                    if (currentTable.factTable[PositiveNegative2DCColumnID[x]][y] >= 0)
                        isPositive = isPositive && true;
                    else
                        isPositive = isPositive && false;

                    if (currentTable.factTable[PositiveNegative2DCColumnID[x]][y] <= 0)
                        isNegative = isNegative && true;
                    else
                        isNegative = isNegative && false;
                }

                if (isPositive == true)
                    factTable[DCcolumnID].Add(0);
                else if (isNegative == true)
                    factTable[DCcolumnID].Add(1);
                else if (isPositive == false && isNegative == false)
                    factTable[DCcolumnID].Add(0);

                for (int x = 0; x < PositiveNegative2DCColumnID.Count; x++)
                {                   
                    if (isPositive == true)
                        factTable[PositiveNegative2DCColumnID[x]].Add(Math.Abs(currentTable.factTable[PositiveNegative2DCColumnID[x]][y]));                   
                    else if (isNegative == true)
                        factTable[PositiveNegative2DCColumnID[x]].Add(Math.Abs(currentTable.factTable[PositiveNegative2DCColumnID[x]][y]));                    
                    else if (isPositive == false && isNegative == false)
                        factTable[PositiveNegative2DCColumnID[x]].Add(currentTable.factTable[PositiveNegative2DCColumnID[x]][y]);                   
                }                
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
        public Dictionary<int, List<double>> NegativePositive2DCSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, List<int> NegativePositive2DCColumnID, int DCcolumnID)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < NegativePositive2DCColumnID.Count; x++)
                factTable.Add(NegativePositive2DCColumnID[x], new List<double>());

            factTable.Add(DCcolumnID, new List<double>());

            bool isPositive;
            bool isNegative;           

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                isPositive = true;
                isNegative = true;

                for (int x = 0; x < NegativePositive2DCColumnID.Count; x++)
                {
                    if (currentTable.factTable[NegativePositive2DCColumnID[x]][y] >= 0)
                        isPositive = isPositive && true;
                    else
                        isPositive = isPositive && false;

                    if (currentTable.factTable[NegativePositive2DCColumnID[x]][y] <= 0)
                        isNegative = isNegative && true;
                    else
                        isNegative = isNegative && false;                   
                }               

                if (isPositive == true)
                    factTable[DCcolumnID].Add(1);
                else if (isNegative == true)
                    factTable[DCcolumnID].Add(0);
                else if (isPositive == false && isNegative == false)
                    factTable[DCcolumnID].Add(1);

                for (int x = 0; x < NegativePositive2DCColumnID.Count; x++)
                {
                    if (isPositive == true)
                        factTable[NegativePositive2DCColumnID[x]].Add(Math.Abs(currentTable.factTable[NegativePositive2DCColumnID[x]][y]));
                    else if (isNegative == true)
                        factTable[NegativePositive2DCColumnID[x]].Add(Math.Abs(currentTable.factTable[NegativePositive2DCColumnID[x]][y]));
                    else if (isPositive == false && isNegative == false)
                        factTable[NegativePositive2DCColumnID[x]].Add(currentTable.factTable[NegativePositive2DCColumnID[x]][y]);
                }

            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
        public LedgerRAM reverseDC(LedgerRAM currentTable, DCsetting currentSetting)
        {
            ConcurrentDictionary<int, List<double>> factTableMultithread = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
           
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
           
            LedgerRAM currentOutput = new LedgerRAM();

            List<double> factTable = new List<double>();
            List<int> rowSegment = new List<int>();
            int DCcolumnID = 0;

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (currentTable.columnName[x] == "D/C")               
                    DCcolumnID = x;
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

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new LedgerRAM2CSVdataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                factTableMultithread[currentSegment] = reverseDCsegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, DCcolumnID);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            factTable.Add(DCcolumnID);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                factTable.AddRange(factTableMultithread[i]);          
            
            for (int i = 0; i < currentTable.columnName.Count; i++)
            {
                if(currentTable.columnName[i].Trim().ToUpper() == "D/C")
                    resultFactTable.Add(DCcolumnID, factTable);
                else
                   resultFactTable.Add(i, currentTable.factTable[i]);
            }

            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
        public List<double> reverseDCsegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, int DCcolumnID)
        {
            List<double> factTable = new List<double>();            

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                if (currentTable.factTable[DCcolumnID][y] == 0)
                    factTable.Add(1);
                else 
                    factTable.Add(0);
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }

    }
}