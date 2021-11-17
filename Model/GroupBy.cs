using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class groupBySetting
    {
        public int rowThread = 100;
        public List<string> groupByColumnName { get; set; }
        public List<string> aggregateFunction { get; set; }
        public List<string> aggregateByColumnName { get; set; }      
    }   

    public class groupBy
    {
        public LedgerRAM groupByList(LedgerRAM currentTable, groupBySetting currentSetting)
        {           
            List<string> distinctNumberColumnName = new List<string>();
            List<string> distinctColumnNameForNumber2Text = new List<string>();
            Dictionary<string, string> removeColumnPrefixDict = new Dictionary<string, string>();

            List<string> aggregateByUpperColumnName = new List<string>();

            for (int x = 0; x < currentSetting.aggregateByColumnName.Count; x++)
                aggregateByUpperColumnName.Add(currentSetting.aggregateByColumnName[x].ToUpper()); 
           
            List<string> groupByUpperColumnName = new List<string>();                                

            for (int x = 0; x < currentSetting.groupByColumnName.Count; x++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.groupByColumnName[x].ToUpper()))
                {
                    if (currentTable.dataType[currentTable.upperColumnName2ID[currentSetting.groupByColumnName[x].ToUpper()]] == "Number")
                    {
                        distinctNumberColumnName.Add(currentSetting.groupByColumnName[x].ToUpper());
                        distinctColumnNameForNumber2Text.Add("Text:" + currentSetting.groupByColumnName[x]);
                        removeColumnPrefixDict.Add("TEXT:" + currentSetting.groupByColumnName[x].ToUpper(), currentSetting.groupByColumnName[x]);                        
                    }
                    else
                        distinctColumnNameForNumber2Text.Add(currentSetting.groupByColumnName[x]);
                }
            }

            currentSetting.groupByColumnName = distinctColumnNameForNumber2Text;

            number2Text newNumber2Text = new number2Text();
            number2TextSetting setNumber2Text = new number2TextSetting();
            setNumber2Text.number2Text = distinctNumberColumnName;

            if (distinctNumberColumnName.Count > 0)
                currentTable = newNumber2Text.number2TextList(currentTable, setNumber2Text);                        
         
            List<int> groupByColumnID = new List<int>();

            for (int x = 0; x < groupByUpperColumnName.Count; x++)
                groupByColumnID.Add(currentTable.upperColumnName2ID[groupByUpperColumnName[x]]);

            for (int x = 0; x < currentSetting.groupByColumnName.Count; x++)            
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.groupByColumnName[x].ToUpper()))                
                    if (currentTable.dataType[currentTable.upperColumnName2ID[currentSetting.groupByColumnName[x].ToUpper()]] != "Number")                                           
                        groupByColumnID.Add(currentTable.upperColumnName2ID[currentSetting.groupByColumnName[x].ToUpper()]);

            Dictionary<string, List<int>> eachStatisticsWithColumnID = new Dictionary<string, List<int>>();
            List<int> numberTypeColumnID = new List<int>();

            for (int x = 0; x < currentSetting.aggregateFunction.Count; x++)
            {
                if (!eachStatisticsWithColumnID.ContainsKey(currentSetting.aggregateFunction[x].Trim()))               
                    if (currentSetting.aggregateFunction[x].ToUpper() != "COUNT")                   
                        eachStatisticsWithColumnID.Add(currentSetting.aggregateFunction[x].Trim(), new List<int>());

                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.aggregateByColumnName[x].ToUpper()))
                {
                    if (currentSetting.aggregateFunction[x].ToUpper() != "COUNT")
                    {
                        if (!numberTypeColumnID.Contains(currentTable.upperColumnName2ID[currentSetting.aggregateByColumnName[x].ToUpper()]))
                            numberTypeColumnID.Add(currentTable.upperColumnName2ID[currentSetting.aggregateByColumnName[x].ToUpper()]);

                        eachStatisticsWithColumnID[currentSetting.aggregateFunction[x].Trim()].Add(currentTable.upperColumnName2ID[currentSetting.aggregateByColumnName[x].ToUpper()]);
                    }
                }
            }           

            // select column for groupByColumnID and numberTypeColumnID
            currentTable = selectColumn(groupByColumnID, numberTypeColumnID, eachStatisticsWithColumnID, currentTable, currentSetting);
           
         //   currentTable.upperColumnName2ID.Clear();

            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();


            for (int x = 0; x < numberTypeColumnID.Count; x++)
            {
              //  currentTable.upperColumnName2ID.Add(currentTable.columnName[numberTypeColumnID[x]].Trim().ToUpper(), x);
                resultUpperColumnName2ID.Add(currentTable.columnName[numberTypeColumnID[x]].Trim().ToUpper(), x);
            }

            List<int> aggregateByColumnID = new List<int>();

            for (int x = 0; x < currentSetting.aggregateByColumnName.Count; x++)
            {
                if (currentSetting.aggregateByColumnName[x].ToUpper() == "NULL")
                    aggregateByColumnID.Add(-999);
                else
                {
                 //   aggregateByColumnID.Add(numberTypeColumnID[currentTable.upperColumnName2ID[currentSetting.aggregateByColumnName[x].ToUpper()]]);
                    aggregateByColumnID.Add(numberTypeColumnID[resultUpperColumnName2ID[currentSetting.aggregateByColumnName[x].ToUpper()]]);
                }
            }          

            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();

            // upperColumnName2ID.Clear();

            Dictionary<string, int> _resultUpperColumnName2ID = new Dictionary<string, int>();

            for (int x = 0; x < groupByColumnID.Count; x++)
            {
                dataType.Add(x, currentTable.dataType[groupByColumnID[x]]);
                columnName.Add(x, currentTable.columnName[groupByColumnID[x]]);
                _resultUpperColumnName2ID.Add(currentTable.columnName[groupByColumnID[x]].ToUpper(), x);
            }         

            for (int x = groupByColumnID.Count; x < (groupByColumnID.Count + aggregateByColumnID.Count); x++)
            {
                dataType.Add(x, "Number");
                if (currentSetting.aggregateFunction[x - groupByColumnID.Count].ToUpper() == "COUNT")
                {
                    columnName.Add(x, currentSetting.aggregateFunction[x - groupByColumnID.Count]);
                    _resultUpperColumnName2ID.Add(currentSetting.aggregateFunction[x - groupByColumnID.Count].ToUpper(), x);
                }
                else
                {
                    if (currentSetting.aggregateFunction[x - groupByColumnID.Count].ToUpper() == "SUM")
                    {
                        columnName.Add(x, currentTable.columnName[aggregateByColumnID[x - groupByColumnID.Count]]);
                        _resultUpperColumnName2ID.Add(currentTable.columnName[aggregateByColumnID[x - groupByColumnID.Count]].ToUpper(), x);
                    }
                    else
                    {
                        columnName.Add(x, currentSetting.aggregateFunction[x - groupByColumnID.Count] + ":" + currentTable.columnName[aggregateByColumnID[x - groupByColumnID.Count]]);
                        _resultUpperColumnName2ID.Add((currentSetting.aggregateFunction[x - groupByColumnID.Count] + ":" + currentTable.columnName[aggregateByColumnID[x - groupByColumnID.Count]]).ToUpper(), x);
                    }
                }
            }
            
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, filter> concurrentRowSegment = new ConcurrentDictionary<int, filter>();
            ConcurrentDictionary<int, Dictionary<int, List<double>>> groupByListOfSegment = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            Dictionary<int, List<double>> groupByList = new Dictionary<int, List<double>>();
            List<int> rowSegment = new List<int>();
            List<string> dataTableColumnName = new List<string>();
            bool isCombineSegment = false;

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

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new filter());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                groupByListOfSegment[currentSegment] = groupByListSegment(aggregateByColumnID, isCombineSegment, rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, groupByColumnID, numberTypeColumnID, eachStatisticsWithColumnID);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int x = 0; x < groupByListOfSegment[0].Count; x++)
            {
                groupByList.Add(x, new List<double>());

                for (int i = 0; i < rowSegment.Count - 1; i++)
                    groupByList[x].AddRange(groupByListOfSegment[i][x]);
            }
            // Combine Segment
            currentTable.factTable = groupByList;

            rowSegment.Clear();
            rowSegment.Add(1);
            rowSegment.Add(groupByList[0].Count);

            isCombineSegment = true;

            currentTable.columnName = columnName;
            currentTable.upperColumnName2ID = _resultUpperColumnName2ID;
            currentTable.dataType = dataType;

            groupByList = groupByListSegment(aggregateByColumnID, isCombineSegment, rowSegment, 0, checkSegmentThreadCompleted, currentTable, currentSetting, groupByColumnID, numberTypeColumnID, eachStatisticsWithColumnID);

            LedgerRAM currentOutput = new LedgerRAM();

            if (currentTable.upperColumnName2ID.ContainsKey("PERIOD END"))           
                currentTable.isPeriodEndExist = true;           

            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = groupByList;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;
            currentOutput.isPeriodEndExist = currentTable.isPeriodEndExist;          

            return currentOutput;
        }       
        public LedgerRAM selectColumn(List<int> groupByColumnID, List<int> numberTypeColumnID, Dictionary<string, List<int>> eachStatisticsWithColumnID, LedgerRAM currentTable, groupBySetting currentSetting)
        {
            LedgerRAM currentLedgerRAM = new LedgerRAM();
            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, int> new2OldGroupByColumnID = new Dictionary<int, int>();  // GroupByTableID map to factTable ID
            Dictionary<int, int> old2NewGroupByColumnID = new Dictionary<int, int>(); 
            Dictionary<string, List<int>> newEachStatisticsWithColumnID = new Dictionary<string, List<int>>();

            int dimensionCount = groupByColumnID.Count;
            int measureCount = numberTypeColumnID.Count;

            for (int x = 0; x < (dimensionCount + measureCount); x++)
            {
                if (x < dimensionCount)
                {
                    if (!new2OldGroupByColumnID.ContainsKey(x))
                        new2OldGroupByColumnID.Add(x, groupByColumnID[x]);

                    if (old2NewGroupByColumnID.ContainsKey(groupByColumnID[x]))
                        old2NewGroupByColumnID.Add(groupByColumnID[x], x);
                }
                else
                {
                    if (!new2OldGroupByColumnID.ContainsKey(x))
                        new2OldGroupByColumnID.Add(x, numberTypeColumnID[x - dimensionCount]);

                    if (!old2NewGroupByColumnID.ContainsKey(numberTypeColumnID[x - dimensionCount]))
                        old2NewGroupByColumnID.Add(numberTypeColumnID[x - dimensionCount], x);
                }
            }
          
            groupByColumnID.Clear(); numberTypeColumnID.Clear();

            for (int x = 0; x < new2OldGroupByColumnID.Count; x++)
            {
                if (x < dimensionCount)
                    groupByColumnID.Add(x);
                else
                    numberTypeColumnID.Add(x);

                var currentID = Convert.ToInt32(new2OldGroupByColumnID[x]);              
                columnName.Add(x, currentTable.columnName[currentID]);
                dataType.Add(x, currentTable.dataType[currentID]);
                factTable.Add(x, currentTable.factTable[currentID]);
                factTable[x][0] = x;

                if (x < dimensionCount)
                {
                    key2Value.Add(x, currentTable.key2Value[currentID]);
                    value2Key.Add(x, currentTable.value2Key[currentID]);
                }
            }


            foreach (var pair in eachStatisticsWithColumnID)
            {
                newEachStatisticsWithColumnID.Add(pair.Key, new List<int>());

                foreach (var columnID in eachStatisticsWithColumnID[pair.Key])
                    newEachStatisticsWithColumnID[pair.Key].Add(old2NewGroupByColumnID[columnID]);
            }

            eachStatisticsWithColumnID.Clear();           

            foreach (var pair in newEachStatisticsWithColumnID)
            {
                eachStatisticsWithColumnID.Add(pair.Key, new List<int>());

                foreach (var columnID in newEachStatisticsWithColumnID[pair.Key])
                    eachStatisticsWithColumnID[pair.Key].Add(columnID);
            }   

            currentLedgerRAM.columnName = columnName;
            currentLedgerRAM.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentLedgerRAM.dataType = dataType;
            currentLedgerRAM.factTable = factTable;
            currentLedgerRAM.key2Value = key2Value;
            currentLedgerRAM.value2Key = value2Key;

            return currentLedgerRAM;
        }
        public Dictionary<int, List<double>> groupByListSegment(List<int> aggregateByColumnID, bool isCombineSegment, List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, groupBySetting currentSetting, List<int> groupByColumnID, List<int> numberTypeColumnID, Dictionary<string, List<int>> eachStatisticsWithColumnID)
        {
            if (isCombineSegment == true)
            {
                aggregateByColumnID.Clear();
                for (int x = 0; x < currentTable.dataType.Count; x++)
                {                   
                    if(currentTable.dataType[x] == "Number")
                        aggregateByColumnID.Add(x);
                }              
            }

            Dictionary<int, List<double>> groupByList = new Dictionary<int, List<double>>();
            int dimensionCount = groupByColumnID.Count;
            int measureCount = aggregateByColumnID.Count;            

            int unique;

            if (currentSegment == 0)
                unique = 0;
            else
                unique = -1;

            for (int x = 0; x < (dimensionCount + measureCount); x++)
                groupByList.Add(x, new List<double>());

            if (currentSegment == 0)
            {
                for (int x = 0; x < (dimensionCount + measureCount); x++)
                    groupByList[x].Add(x); // add columnID at row 0           
            }

            List <double> elementCount = new List<double>();
            for (int x = dimensionCount; x > 0; x--)
                elementCount.Add(currentTable.key2Value[groupByColumnID[x - 1]].Count); // master item for each groupBy column

            List<double> logElementCount = new List<double>();

            for (int x = dimensionCount - 1; x > 0; x--)
            {
                if (x == dimensionCount - 1) logElementCount.Add(Math.Round((Math.Log(currentTable.key2Value[groupByColumnID[x]].Count, 10) + 0.5000001), 0));
                if (x < dimensionCount - 1) logElementCount.Add(Math.Round((Math.Log(currentTable.key2Value[groupByColumnID[x]].Count, 10) + 0.5000001), 0) + logElementCount[groupByColumnID.Count - 2 - x]);
            }

            List<double> factor = new List<double>();

            for (int x = 0; x < (dimensionCount - 1); x++)
                factor.Add(Math.Pow(10, logElementCount[x]));

            Dictionary<decimal, int> groupByColumnIDChecksumList = new Dictionary<decimal, int>();

            decimal groupByColumnIDChecksum;

            Dictionary<string, int> upperAggregateFunctionDict = new Dictionary<string, int>();
            for (int x = 0; x < currentSetting.aggregateFunction.Count; x++)
                if(!upperAggregateFunctionDict.ContainsKey(currentSetting.aggregateFunction[x].ToUpper()))
                    upperAggregateFunctionDict.Add(currentSetting.aggregateFunction[x].ToUpper(), x);

            if (upperAggregateFunctionDict.ContainsKey("SUM") & upperAggregateFunctionDict.Count == 1)
            {

                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    groupByColumnIDChecksum = 0;

                    for (int x = 0; x < dimensionCount; x++) // convert multiple dimension value to an unique number 
                    {
                        if (x < dimensionCount - 1) groupByColumnIDChecksum = groupByColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[groupByColumnID[x]][y] * factor[groupByColumnID.Count - 2 - x]);
                        if (x == dimensionCount - 1) groupByColumnIDChecksum = groupByColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[groupByColumnID[x]][y]);
                    }

                    if (!groupByColumnIDChecksumList.ContainsKey(groupByColumnIDChecksum))
                    {
                        unique++;
                        groupByColumnIDChecksumList.Add(groupByColumnIDChecksum, unique);

                        for (int x = 0; x < dimensionCount; x++)
                            groupByList[x].Add(currentTable.factTable[groupByColumnID[x]][y]); // add dimension value for first unique item                   

                        for (int x = dimensionCount; x < (dimensionCount + currentSetting.aggregateFunction.Count); x++)
                            groupByList[x].Add(currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]); // add measure number by first unique dimension value                        
                    }
                    else // addition = current value + last value of the same key
                    {
                        var add = groupByColumnIDChecksumList[groupByColumnIDChecksum];

                        for (int x = dimensionCount; x < (dimensionCount + currentSetting.aggregateFunction.Count); x++)                           
                             groupByList[x][add] = Math.Round((double)(groupByList[x][add] + currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]), 2);
                    }
                }
            }
            else
            {
                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
                {
                    groupByColumnIDChecksum = 0;

                    for (int x = 0; x < dimensionCount; x++) // convert multiple dimension value to an unique number 
                    {
                        if (x < dimensionCount - 1) groupByColumnIDChecksum = groupByColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[groupByColumnID[x]][y] * factor[groupByColumnID.Count - 2 - x]);
                        if (x == dimensionCount - 1) groupByColumnIDChecksum = groupByColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[groupByColumnID[x]][y]);
                    }

                    if (!groupByColumnIDChecksumList.ContainsKey(groupByColumnIDChecksum))
                    {
                        unique++;
                        groupByColumnIDChecksumList.Add(groupByColumnIDChecksum, unique);

                        for (int x = 0; x < dimensionCount; x++)
                            groupByList[x].Add(currentTable.factTable[groupByColumnID[x]][y]); // add dimension value for first unique item                   

                        for (int x = dimensionCount; x < (dimensionCount + currentSetting.aggregateFunction.Count); x++)
                        {
                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() != "COUNT")
                                groupByList[x].Add(currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]); // add measure number by first unique dimension value                        

                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() == "COUNT")
                                if (isCombineSegment == true)
                                    groupByList[x].Add(currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]); // add measure number by first unique dimension value 
                                else
                                    groupByList[x].Add(1);
                        }
                    }
                    else // addition = current value + last value of the same key
                    {
                        var add = groupByColumnIDChecksumList[groupByColumnIDChecksum];

                        for (int x = dimensionCount; x < (dimensionCount + currentSetting.aggregateFunction.Count); x++)
                        {
                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() == "SUM")
                                groupByList[x][add] = Math.Round((double)(groupByList[x][add] + currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]), 2);

                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() == "COUNT")
                                if (isCombineSegment == true)
                                    groupByList[x][add] = Math.Round((double)(groupByList[x][add] + currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y]), 2);
                                else
                                    groupByList[x][add] = groupByList[x][add] + 1;

                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() == "MAX")
                                if (currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y] > groupByList[x][add])
                                    groupByList[x][add] = currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y];

                            if (currentSetting.aggregateFunction[x - dimensionCount].ToUpper() == "MIN")
                                if (currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y] < groupByList[x][add])
                                    groupByList[x][add] = currentTable.factTable[aggregateByColumnID[x - dimensionCount]][y];
                        }
                    }
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return groupByList;    
        }
    }
}

