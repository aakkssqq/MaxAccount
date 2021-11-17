using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class table2CellSetting
    {
        public int rowThread = 100;
        public string calc2Cell { get; set; }
        public List<string> refColumnName { get; set; }
        
        public int decimalPlace = 999;
    }

    public class table2Cell
    {
        public string cellAddress(LedgerRAM currentTable, table2CellSetting currentSetting)
        {          
            string row = currentSetting.refColumnName[1].ToUpper();
            string column = currentSetting.refColumnName[0].ToUpper();
            string currentText;          

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            int columnID = upperColumnName2ID[column];

            if (row == "FIRST")
                row = "1";

            if (row == "LAST")           
                row = (currentTable.factTable[columnID].Count - 1).ToString();

            bool success = Int32.TryParse(row, out int _row);           

            if (success == true)
            {
                if (currentTable.dataType[columnID] != "Number")                              
                    currentText = currentTable.key2Value[columnID][currentTable.factTable[columnID][_row]].ToString().Trim();
                else
                    currentText = currentTable.factTable[columnID][_row].ToString().Trim();               
            }
            else
                currentText = "Error";

            return currentText;
        }
        public double table2CellProcess(LedgerRAM currentTable, table2CellSetting currentSetting)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            Dictionary<string, int> upperRefColumnName = new Dictionary<string, int>();
            List<int> refColumnID = new List<int>();

            foreach (var pair in currentSetting.refColumnName)
                upperRefColumnName.Add(pair.ToUpper(), 0);

            for (int x = 0; x < currentTable.columnName.Count; x++)           
                if (upperRefColumnName.ContainsKey(currentTable.columnName[x].ToUpper()))              
                    refColumnID.Add(x);

            ConcurrentDictionary<int, List<double>> factTableMultithread = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, table2Cell> concurrentRowSegment = new ConcurrentDictionary<int, table2Cell>();
            ConcurrentDictionary<int, double> currentNumberMultithread = new ConcurrentDictionary<int, double>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            List<int> rowSegment = new List<int>();
            double currentNumber = 0;            

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

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) 
                concurrentRowSegment.TryAdd(worker, new table2Cell());            

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {
                if (currentSetting.calc2Cell.ToUpper() == "SUM" || currentSetting.calc2Cell.ToUpper() == "AVERAGE")               
                    currentNumberMultithread.TryAdd(currentSegment, sum(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, refColumnID));

                if (currentSetting.calc2Cell.ToUpper() == "MAX")
                    currentNumberMultithread.TryAdd(currentSegment, max(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, refColumnID));

                if (currentSetting.calc2Cell.ToUpper() == "MIN")
                    currentNumberMultithread.TryAdd(currentSegment, min(rowSegment, currentSegment, checkSegmentThreadCompleted, currentTable, currentSetting, refColumnID));
            });

            if (currentSetting.calc2Cell.ToUpper() != "COUNT")
            {
                do
                {
                    Thread.Sleep(5);
                } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
            }

            if (currentSetting.calc2Cell.ToUpper() == "SUM" || currentSetting.calc2Cell.ToUpper() == "AVERAGE")
            {
                for (int i = 0; i < rowSegment.Count - 1; i++)
                    currentNumber = currentNumber + currentNumberMultithread[i];
            }

            if (currentSetting.calc2Cell.ToUpper() == "COUNT")
            {
                for (int x = 0; x < refColumnID.Count; x++)
                    currentNumber = currentNumber + currentTable.factTable[refColumnID[x]].Count - 1;
            }

            if (currentSetting.calc2Cell.ToUpper() == "AVERAGE")
            {
                var currentCount = 0;

                for (int x = 0; x < refColumnID.Count; x++)
                    currentCount = currentCount + currentTable.factTable[refColumnID[x]].Count - 1;

                currentNumber = currentNumber / currentCount;                

                if (currentSetting.decimalPlace != 999)
                    currentNumber = Math.Round((double)(currentNumber), currentSetting.decimalPlace);
            }

            if (currentSetting.calc2Cell.ToUpper() == "MAX")
            {
                for (int i = 0; i < rowSegment.Count - 1; i++)
                    if(currentNumberMultithread[i] > currentNumber)
                        currentNumber = currentNumberMultithread[i];
            }

            if (currentSetting.calc2Cell.ToUpper() == "MIN")
            {
                currentNumber = 9999999999999999999;

                for (int i = 0; i < rowSegment.Count - 1; i++)
                    if (currentNumberMultithread[i] < currentNumber)
                        currentNumber = currentNumberMultithread[i];
            }

            return currentNumber;
        }
        public double sum(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, table2CellSetting currentSetting, List<int>refColumnID)
        {
            double currentNumber = 0;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < refColumnID.Count; x++)
                    currentNumber = currentNumber + currentTable.factTable[refColumnID[x]][y];
            }

            if (currentSetting.calc2Cell.ToUpper() != "AVERAGE")
            {
                if (currentSetting.decimalPlace != 999)
                    currentNumber = Math.Round((double)(currentNumber), currentSetting.decimalPlace);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return currentNumber;
        }        
        public double max(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, table2CellSetting currentSetting, List<int> refColumnID)
        {
            double currentNumber = 0;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < refColumnID.Count; x++)
                    if (currentTable.factTable[refColumnID[x]][y] > currentNumber)
                        currentNumber = currentTable.factTable[refColumnID[x]][y];
            }

            if (currentSetting.decimalPlace != 999)
                currentNumber = Math.Round((double)(currentNumber), currentSetting.decimalPlace);

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return currentNumber;
        }
        public double min(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, LedgerRAM currentTable, table2CellSetting currentSetting, List<int> refColumnID)
        {
            double currentNumber = 9999999999999999999;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                for (int x = 0; x < refColumnID.Count; x++)
                    if (currentTable.factTable[refColumnID[x]][y] < currentNumber)
                        currentNumber = currentTable.factTable[refColumnID[x]][y];
            }

            if (currentSetting.decimalPlace != 999)
                currentNumber = Math.Round((double)(currentNumber), currentSetting.decimalPlace);

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return currentNumber;
        }
    }
}
