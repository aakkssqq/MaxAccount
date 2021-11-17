using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class dinstinctSetting
    {
        public int rowThread = 100;
        public List<string> distinctColumnName { get; set; }
    }

    public class distinct
    {
        public LedgerRAM distinctList(LedgerRAM currentTable, dinstinctSetting currentSetting)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);           

            List<string> distinctNumberColumnName = new List<string>();
            List<string> distinctColumnNameForNumber2Text = new List<string>();
            Dictionary<string, string> removeColumnPrefixDict = new Dictionary<string, string>();

            for (int x = 0; x < currentSetting.distinctColumnName.Count; x++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.distinctColumnName[x].ToUpper()))
                {
                    if (currentTable.dataType[currentTable.upperColumnName2ID[currentSetting.distinctColumnName[x].ToUpper()]] == "Number")
                    {
                        distinctNumberColumnName.Add(currentSetting.distinctColumnName[x].ToUpper());
                        distinctColumnNameForNumber2Text.Add("Text:" + currentSetting.distinctColumnName[x]);
                        removeColumnPrefixDict.Add("TEXT:" + currentSetting.distinctColumnName[x].ToUpper(), currentSetting.distinctColumnName[x]);
                    }
                    else                    
                        distinctColumnNameForNumber2Text.Add(currentSetting.distinctColumnName[x]);
                }
            }

            currentSetting.distinctColumnName = distinctColumnNameForNumber2Text;

            number2Text newNumber2Text = new number2Text();
            number2TextSetting setNumber2Text = new number2TextSetting();
            setNumber2Text.number2Text = distinctNumberColumnName;

            if (distinctNumberColumnName.Count > 0)
               currentTable = newNumber2Text.number2TextList(currentTable, setNumber2Text);

            List<string> distinctColumnName = new List<string>();
            List<int> distinctColumnID = new List<int>();          

            for (int x = 0; x < currentSetting.distinctColumnName.Count; x++)
            {              
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.distinctColumnName[x].ToUpper()))
                {
                    if (currentTable.dataType[currentTable.upperColumnName2ID[currentSetting.distinctColumnName[x].ToUpper()]] != "Number")
                    {
                        distinctColumnName.Add(currentSetting.distinctColumnName[x].ToUpper());
                        distinctColumnID.Add(currentTable.upperColumnName2ID[currentSetting.distinctColumnName[x].ToUpper()]);
                    }
                }
            }
            
            //select column for distinct
            currentTable = selectColumn(distinctColumnID, currentTable);
          
            LedgerRAM currentOutput = new LedgerRAM();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, filter> concurrentRowSegment = new ConcurrentDictionary<int, filter>();
            ConcurrentDictionary<int, Dictionary<int, List<double>>> distinctListOfSegment = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            Dictionary<int, List<double>> distinctList = new Dictionary<int, List<double>>();
           
            List<int> rowSegment = new List<int>();
            List<string> dataTableColumnName = new List<string>();          

            rowSegment.Add(1);
            if (currentTable.factTable[0].Count > 10000)
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
                distinctListOfSegment[currentSegment] = distinctListSegment(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, distinctColumnID);             
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);                  

            for (int x = 0; x < distinctListOfSegment[0].Count; x++)
            {
                distinctList.Add(x, new List<double>());

                for (int i = 0; i < rowSegment.Count - 1; i++)
                    distinctList[x].AddRange(distinctListOfSegment[i][x]);
            }
           
            currentTable.factTable = distinctList;
           
            rowSegment.Clear();
            rowSegment.Add(1);
            rowSegment.Add(distinctList[0].Count);

            distinctList = distinctListSegment(rowSegment, 0, checkSegmentThreadCompleted, currentTable, currentSetting, distinctColumnID);

            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
           
            foreach (var pair in currentTable.columnName)
            {
                if (removeColumnPrefixDict.ContainsKey(pair.Value.ToUpper()))
                {                    
                    resultColumnName.Add(pair.Key, removeColumnPrefixDict[pair.Value.ToUpper()]);
                    resultUpperColumnName2ID.Add(removeColumnPrefixDict[pair.Value.ToUpper()].ToUpper(), pair.Key);
                }
                else
                {
                    resultColumnName.Add(pair.Key, pair.Value);
                    resultUpperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);
                }
            }           

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = distinctList;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;                     

            return currentOutput;
        }
        public LedgerRAM selectColumn(List<int> distinctColumnID, LedgerRAM currentTable)
        {
            LedgerRAM currentLedgerRAM = new LedgerRAM();           

            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, int> new2OldDistinctColumnID = new Dictionary<int, int>();

            for (int x = 0; x < distinctColumnID.Count; x++)
                new2OldDistinctColumnID.Add(x, distinctColumnID[x]);
          
            distinctColumnID.Clear();

            for (int x = 0; x < new2OldDistinctColumnID.Count; x++)
            {
                distinctColumnID.Add(x);
                var currentID = Convert.ToInt32(new2OldDistinctColumnID[x]);
                columnName.Add(x, currentTable.columnName[currentID]);
                upperColumnName2ID.Add(currentTable.columnName[currentID].ToUpper(), x);
                dataType.Add(x, currentTable.dataType[currentID]);
                factTable.Add(x, currentTable.factTable[currentID]);
                key2Value.Add(x, currentTable.key2Value[currentID]);
                value2Key.Add(x, currentTable.value2Key[currentID]);                               
            }          

            currentLedgerRAM.columnName = columnName;
            currentLedgerRAM.upperColumnName2ID = upperColumnName2ID;
            currentLedgerRAM.dataType = dataType;
            currentLedgerRAM.factTable = factTable;
            currentLedgerRAM.key2Value = key2Value;
            currentLedgerRAM.value2Key = value2Key;

            return currentLedgerRAM;
        }
        public Dictionary<int, List<double>> distinctListSegment(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, dinstinctSetting currentSetting, List<int> distinctColumnID)
        {
            Dictionary<int, List<double>> distinctList = new Dictionary<int, List<double>>();          

            int unique = 0;

            for (int x = 0; x < distinctColumnID.Count; x++)
                distinctList.Add(x, new List<double>());
            
            if (currentSegment == 0)
            {
                for (int x = 0; x < distinctColumnID.Count; x++)
                    distinctList[x].Add(x); // add columnID at row 0           
            }

            List<double> elementCount = new List<double>();
            for (int x = distinctColumnID.Count; x > 0; x--)
                elementCount.Add(currentTable.key2Value[distinctColumnID[x - 1]].Count); // master item for each distinct column

            List<double> logElementCount = new List<double>();

            for (int x = distinctColumnID.Count - 1; x > 0; x--)
            {
                if (x == distinctColumnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(currentTable.key2Value[distinctColumnID[x]].Count, 10) + 0.5000001), 0));
                if (x < distinctColumnID.Count - 1) logElementCount.Add(Math.Round((Math.Log(currentTable.key2Value[distinctColumnID[x]].Count, 10) + 0.5000001), 0) + logElementCount[distinctColumnID.Count - 2 - x]);
            }

            List<double> factor = new List<double>();
            for (int x = 0; x < (distinctColumnID.Count - 1); x++)
                factor.Add(Math.Pow(10, logElementCount[x]));

            Dictionary<decimal, int> distinctColumnIDChecksumList = new Dictionary<decimal, int>();

            decimal distinctColumnIDChecksum;
           
            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                distinctColumnIDChecksum = 0;
                distinctSum(y);
            }

            void distinctSum(int y)
            {
                for (int x = 0; x < distinctColumnID.Count; x++) // convert multiple dimension value to an unique number 
                {                   
                    if (x < distinctColumnID.Count - 1) distinctColumnIDChecksum = distinctColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[distinctColumnID[x]][y] * factor[distinctColumnID.Count - 2 - x]);
                    if (x == distinctColumnID.Count - 1) distinctColumnIDChecksum = distinctColumnIDChecksum + Convert.ToDecimal(currentTable.factTable[distinctColumnID[x]][y]);
                }

                if (!distinctColumnIDChecksumList.ContainsKey(distinctColumnIDChecksum))
                {
                    unique++;
                    distinctColumnIDChecksumList.Add(distinctColumnIDChecksum, unique);

                    for (int x = 0; x < distinctColumnID.Count; x++)
                        distinctList[x].Add(currentTable.factTable[distinctColumnID[x]][y]); // add dimension value for first unique item                   
                }
                else // addition = current value + last value of the same key
                {
                    // distinct only without sum on amount                     
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return distinctList;
        }
    }
}

