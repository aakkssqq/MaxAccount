using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class reverseNumberSetting
    {
        public int rowThread = 100;
        public List<string> numberTypeColumnName { get; set; }
    }

    public class reverseNumber
    {
        public LedgerRAM reverseNumberProcess(LedgerRAM currentTable, reverseNumberSetting currentSetting)
        {
            ConcurrentDictionary<int, Dictionary<int, List<double>>> factTableMultithread = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            ConcurrentDictionary<int, LedgerRAM2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, LedgerRAM2CSVdataFlow>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            List<int> rowSegment = new List<int>();
            List<string> numberTypeColumnName = new List<string>();           
            List<int> numberTypeColumnID = new List<int>();

            for (int x = 0; x < currentSetting.numberTypeColumnName.Count; x++)
            {
                if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.numberTypeColumnName[x].ToUpper()))
                {
                    numberTypeColumnID.Add(currentTable.upperColumnName2ID[currentSetting.numberTypeColumnName[x].ToUpper()]);                   
                }
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
                factTableMultithread[currentSegment] = negative(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, numberTypeColumnID);
            });

            do
            {
                Thread.Sleep(5);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < numberTypeColumnID.Count; x++)
            {
                resultFactTable.Add(numberTypeColumnID[x], new List<double>());

                for (int s = 0; s < rowSegment.Count - 1; s++)
                    resultFactTable[numberTypeColumnID[x]].AddRange(factTableMultithread[s][numberTypeColumnID[x]]);
            }

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                if (!numberTypeColumnID.Contains(x))
                    resultFactTable.Add(x, currentTable.factTable[x]);
            }

            LedgerRAM currentOutput = new LedgerRAM();
            currentOutput.columnName = currentTable.columnName;
            currentOutput.upperColumnName2ID = currentTable.upperColumnName2ID;
            currentOutput.dataType = currentTable.dataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = currentTable.key2Value;
            currentOutput.value2Key = currentTable.value2Key;

            return currentOutput;
        }
        public Dictionary<int, List<double>> negative(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, List<int> numberTypeColumnID)
        {
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();

            for (int x = 0; x < numberTypeColumnID.Count; x++)
            {
                factTable.Add(numberTypeColumnID[x], new List<double>());

                if(currentSegment == 0)
                    factTable[numberTypeColumnID[x]].Add(numberTypeColumnID[x]);

                for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)                
                    factTable[numberTypeColumnID[x]].Add(currentTable.factTable[numberTypeColumnID[x]][y] * -1);               
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return factTable;
        }
    }
}

