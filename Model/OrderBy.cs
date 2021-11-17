using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class orderBySetting
    {
        public int rowThread = 100;
        public int columnThread = 100;
        public Dictionary<string, string> orderByColumnName { get; set; }
    }

    public class orderBy
    {
        public LedgerRAM orderByList(LedgerRAM currentTable, orderBySetting currentSetting)
        {              
            Dictionary<int, string> orderByColumnID = new Dictionary<int, string>();
            List<int> orderByColumnIDList = new List<int>();

            LedgerRAM currentOutput = new LedgerRAM();
           
            foreach (var pair in currentSetting.orderByColumnName)
            {
                /*
                do
                {
                    Thread.Sleep(2);
                    Console.WriteLine("----");

                } while (!currentTable.upperColumnName2ID.ContainsKey(pair.Key.Trim().ToUpper()));                
                */
                orderByColumnID.Add(currentTable.upperColumnName2ID[pair.Key.Trim().ToUpper()], pair.Value);
                orderByColumnIDList.Add(currentTable.upperColumnName2ID[pair.Key.Trim().ToUpper()]);                
            }
           
            Dictionary<int, Dictionary<double, double>> numberKey2Value = new Dictionary<int, Dictionary<double, double>>();          

            for (int x = 0; x < orderByColumnIDList.Count; x++)
            {   
                if (currentTable.dataType[orderByColumnIDList[x]] == "Number")
                {
                    numberKey2Value.Add(orderByColumnIDList[x], new Dictionary<double, double>());

                    for (int y = 1; y < currentTable.factTable[orderByColumnIDList[x]].Count; y++)

                        if(!numberKey2Value[orderByColumnIDList[x]].ContainsKey(currentTable.factTable[orderByColumnIDList[x]][y]))
                            numberKey2Value[orderByColumnIDList[x]].Add(currentTable.factTable[orderByColumnIDList[x]][y], y);
                }
            }

            Dictionary<int, Dictionary<double, double>> ramKey2Order = new Dictionary<int, Dictionary<double, double>>();        
            Dictionary<int, Dictionary<double, double>> ramNumberKey2Order = new Dictionary<int, Dictionary<double, double>>();
            int order = 0; // sorting based on reorganised and distinct dimension

            foreach (var pair in orderByColumnID)           
            {               
                order = 0;                
                
                if (currentTable.dataType[pair.Key] == "Number")
                {
                    ramNumberKey2Order.Add(pair.Key, new Dictionary<double, double>());                 

                    if (pair.Value == "A")
                    {
                        foreach (var item in numberKey2Value[pair.Key].OrderBy(j => j.Key))
                        {
                            order++;
                            ramNumberKey2Order[pair.Key].Add(item.Key, order);                            
                        }
                    }
                    if (pair.Value == "D")
                    {
                        foreach (var item in numberKey2Value[pair.Key].OrderByDescending(j => j.Key))
                        {
                            order++;
                            ramNumberKey2Order[pair.Key].Add(item.Key, order);                          
                        }
                    }
                }

                if (currentTable.dataType[pair.Key] != "Number")
                {
                    ramKey2Order.Add(pair.Key, new Dictionary<double, double>());
                    
                    if (pair.Value == "A")
                    {
                        foreach (var item in currentTable.key2Value[pair.Key].OrderBy(j => j.Value))
                        {
                            order++;
                            ramKey2Order[pair.Key].Add(item.Key, order);                       
                        }
                    }
                    if (pair.Value == "D")
                    {
                        foreach (var item in currentTable.key2Value[pair.Key].OrderByDescending(j => j.Value))
                        {
                            order++;
                            ramKey2Order[pair.Key].Add(item.Key, order);                         
                        }
                    }
                }
            }

            Dictionary<int, List<double>> numberTypeFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> sortedNumberTypeFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<double, double>> revisedNumberKey2Value = new Dictionary<int, Dictionary<double, double>>();

            List<int> columnID = new List<int>();

            for (int x = 0; x < orderByColumnIDList.Count; x++)
            {
                order = 0;
                columnID.Add(x);             
                factTable.Add(x, new List<double>());
                factTable[x].Add(x);

                if (currentTable.dataType[orderByColumnIDList[x]] == "Number")
                {                    
                    revisedNumberKey2Value.Add(x, new Dictionary<double, double>());

                    for (int y = 1; y < currentTable.factTable[orderByColumnIDList[x]].Count; y++)
                        factTable[x].Add(ramNumberKey2Order[orderByColumnIDList[x]][currentTable.factTable[orderByColumnIDList[x]][y]]);

                    foreach (var pair in numberKey2Value[orderByColumnIDList[x]])                 
                        revisedNumberKey2Value[x].Add(ramNumberKey2Order[orderByColumnIDList[x]][pair.Key], pair.Value);                   
                }
       
                if (currentTable.dataType[orderByColumnIDList[x]] != "Number")
                {                                                       
                    key2Value.Add(x, new Dictionary<double, string>());

                    for (int y = 1; y < currentTable.factTable[orderByColumnIDList[x]].Count; y++)
                        factTable[x].Add(ramKey2Order[orderByColumnIDList[x]][currentTable.factTable[orderByColumnIDList[x]][y]]);

                    foreach (var pair in currentTable.key2Value[orderByColumnIDList[x]])
                        key2Value[x].Add(ramKey2Order[orderByColumnIDList[x]][pair.Key], pair.Value);                    
                }
            }
         
            orderBy currentOrderBy = new orderBy();

            Dictionary<int, decimal> checkSumList = currentOrderBy.checkSumList(orderByColumnIDList, currentTable, factTable, key2Value, columnID, revisedNumberKey2Value);
          
            var sortedCheckSumList = from pair in checkSumList
                        orderby pair.Value ascending
                        select pair;

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            Dictionary<int, List<double>> sortedFactTable = new Dictionary<int, List<double>>();

            ConcurrentDictionary<int, orderBy> writeColumnThread = new ConcurrentDictionary<int, orderBy>();

            for (int worker = 0; worker < currentTable.columnName.Count; worker++)
            {
                writeColumnThread.TryAdd(worker, new orderBy());
                sortedFactTable.Add(worker, new List<double>());
            }

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, currentTable.columnName.Count, options, x =>
            {       
                sortedFactTable[x] = factTableSegment(x, checkThreadCompleted, currentTable, sortedCheckSumList);               
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < currentTable.columnName.Count);

            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = sortedFactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
        public List<double> factTableSegment(int x, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, IOrderedEnumerable<KeyValuePair<int, decimal>> sortedCheckSumList)
        {
           List<double> sortedFactTable = new List<double>();

            sortedFactTable.Add(x);

            foreach (var pair in sortedCheckSumList)
                sortedFactTable.Add(currentTable.factTable[x][pair.Key]); // pair.Key is key of factTable
           
            checkThreadCompleted.Enqueue(x);

            return sortedFactTable;
        }
        public Dictionary<int, decimal> checkSumList(List<int> orderByColumnIDList, LedgerRAM currentTable, Dictionary<int, List<double>> factTable, Dictionary<int, Dictionary<double, string>> key2Value, List<int> columnID, Dictionary<int, Dictionary<double, double>> revisedNumberKey2Value)
        {
            Dictionary<int, List<double>> currentdistinct = new Dictionary<int, List<double>>();

            decimal selectedColumnIDChecksum;
            Dictionary<int, decimal> selectedColumnIDChecksumList = new Dictionary<int, decimal>();

            for (int x = 0; x < columnID.Count; x++)
                currentdistinct.Add(x, new List<double>());

            for (int x = 0; x < columnID.Count; x++)
                currentdistinct[x].Add(factTable[columnID[x]][0]);

            List<double> elementCount = new List<double>();
            for (int x = columnID.Count; x > 0; x--)
            {
                if (currentTable.dataType[orderByColumnIDList[x - 1]] == "Number")
                    elementCount.Add(revisedNumberKey2Value[columnID[x - 1]].Count);
                else
                    elementCount.Add(key2Value[columnID[x - 1]].Count);
            }

            List<double> logElementCount = new List<double>();

            for (int x = columnID.Count - 1; x > 0; x--)
            {
                if (currentTable.dataType[orderByColumnIDList[x]] == "Number")
                {
                    if (x == columnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(revisedNumberKey2Value[columnID[x]].Count, 10) + 0.5000001), 0));
                    if (x < columnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(revisedNumberKey2Value[columnID[x]].Count, 10) + 0.5000001), 0) + logElementCount[columnID.Count - 2 - x]);

                }
                else
                {
                    if (x == columnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(key2Value[columnID[x]].Count, 10) + 0.5000001), 0));
                    if (x < columnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(key2Value[columnID[x]].Count, 10) + 0.5000001), 0) + logElementCount[columnID.Count - 2 - x]);
                }
            }

            List<double> factor = new List<double>();
            for (int x = 0; x < (columnID.Count - 1); x++)
                factor.Add(Math.Pow(10, logElementCount[x]));

            for (int y = 1; y < factTable[0].Count; y++)
            {
                selectedColumnIDChecksum = 0;
                for (int x = 0; x < columnID.Count; x++)
                {
                    if (x < columnID.Count - 1) selectedColumnIDChecksum = selectedColumnIDChecksum + Convert.ToDecimal(factTable[columnID[x]][y] * factor[columnID.Count - 2 - x]);
                    if (x == columnID.Count - 1) selectedColumnIDChecksum = selectedColumnIDChecksum + Convert.ToDecimal(factTable[columnID[x]][y]);
                }

                selectedColumnIDChecksumList.Add(y, selectedColumnIDChecksum);

                for (int x = 0; x < columnID.Count; x++)
                    currentdistinct[x].Add(factTable[columnID[x]][y]);
            }
            return selectedColumnIDChecksumList;
        }
    }
}