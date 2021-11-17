using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace MaxAccount
{
    public class copyTableSetting
    {
        public int rowThread = 100;
        public int columnThread = 100;
        public string sourceTable { get; set; }
        public string resultTable { get; set; }
        public string commonTable { get; set; }
    }

    public class copyTable
    {
        public LedgerRAM copyTableProcess(LedgerRAM currentTable)
        {  
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
           
            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                resultColumnName.Add(x, currentTable.columnName[x]);
                resultUpperColumnName2ID.Add(currentTable.columnName[x].ToUpper(), x);
                resultDataType.Add(x, currentTable.dataType[x]);
                resultFactTable.Add(x, new List<double>());

                resultFactTable[x].AddRange(currentTable.factTable[x]);

                if (currentTable.dataType[x] != "Number")
                {
                    resultKey2Value.Add(x, currentTable.key2Value[x]);
                    resultValue2Key.Add(x, currentTable.value2Key[x]);
                }
            }          

            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput.columnName = resultColumnName;
            currentOutput.upperColumnName2ID = resultUpperColumnName2ID;
            currentOutput.dataType = resultDataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = resultKey2Value;
            currentOutput.value2Key = resultValue2Key;

            return currentOutput;
        }

        public LedgerRAM copyTableCommonTableProcess(Dictionary<string, LedgerRAM> ramStore, copyTableSetting currentSetting)
        {
            ConcurrentDictionary<int, List<double>> factTable = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, Dictionary<double, string>> key2Value = new ConcurrentDictionary<int, Dictionary<double, string>>();
            ConcurrentDictionary<int, Dictionary<string, double>> value2Key = new ConcurrentDictionary<int, Dictionary<string, double>>();

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();

            for (int i = 0; i < ramStore[currentSetting.sourceTable].dataType.Count; i++)
            {
                factTable.TryAdd(i, new List<double>());

                if (ramStore[currentSetting.sourceTable].dataType[i] != "Number")
                {
                    key2Value.TryAdd(i, new Dictionary<double, string>());
                    value2Key.TryAdd(i, new Dictionary<string, double>());
                }
            }

            ConcurrentDictionary<int, copyTable> writeColumnThread = new ConcurrentDictionary<int, copyTable>();

            for (int worker = 0; worker < ramStore[currentSetting.sourceTable].dataType.Count; worker++)
                writeColumnThread.TryAdd(worker, new copyTable());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            int u = 0;

            var columnCount = ramStore[currentSetting.sourceTable].dataType.Count;
          
            for (int x = 0; x < columnCount; x++)
            {
                if (ramStore[currentSetting.sourceTable].dataType[x] != "Number")
                {                    
                   (factTable[x], key2Value[x], value2Key[x]) = commonkey2ValueSegment(ramStore, x, checkThreadCompleted, currentSetting);

                    u++;
                }   
            }

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < u);

            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            for (int x = 0; x < ramStore[currentSetting.sourceTable].dataType.Count; x++)
            {
                if (ramStore[currentSetting.sourceTable].dataType[x] != "Number")
                    resultFactTable.Add(x, factTable[x]);
                else
                    resultFactTable.Add(x, ramStore[currentSetting.sourceTable].factTable[x]);
            }

            LedgerRAM currentOutput = new LedgerRAM();

            currentOutput.columnName = ramStore[currentSetting.sourceTable].columnName;
            currentOutput.upperColumnName2ID = ramStore[currentSetting.sourceTable].upperColumnName2ID;
            currentOutput.dataType = ramStore[currentSetting.sourceTable].dataType;
            currentOutput.factTable = resultFactTable;
            currentOutput.key2Value = new Dictionary<int, Dictionary<double, string>>(key2Value); 
            currentOutput.value2Key = new Dictionary<int, Dictionary<string, double>>(value2Key); 

            return currentOutput;
        }

        public (List<double> factTable, Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) commonkey2ValueSegment(Dictionary<string, LedgerRAM> ramStore, int x, ConcurrentQueue<int> checkThreadCompleted, copyTableSetting currentSetting)
        {
            List<double> factTable = new List<double>();           
            factTable.Add(x); 
            double count;

            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            if (ramStore[currentSetting.commonTable].key2Value.ContainsKey(x))
            {
                key2Value = ramStore[currentSetting.commonTable].key2Value[x];
                value2Key = ramStore[currentSetting.commonTable].value2Key[x];
            }

            for (int y = 1; y < ramStore[currentSetting.sourceTable].factTable[0].Count; y++)
            {
                string text = ramStore[currentSetting.sourceTable].key2Value[x][ramStore[currentSetting.sourceTable].factTable[x][y]];

                if (value2Key.ContainsKey(text)) 
                    factTable.Add(value2Key[text]);

                if (!value2Key.ContainsKey(text))
                {  
                    count = value2Key.Count;                   
                    key2Value.Add(count, text);
                    value2Key.Add(text, count);
                    factTable.Add(count);
                }
            }

            checkThreadCompleted.Enqueue(x);
            return (factTable, key2Value, value2Key);
        }

    }
}
