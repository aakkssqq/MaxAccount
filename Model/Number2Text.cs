using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class number2TextSetting
    {        
        public int columnThread = 100;
        public List<string> number2Text { get; set; }        
    }

    public class number2Text
    {
        public LedgerRAM number2TextList(LedgerRAM currentTable, number2TextSetting currentSetting)
        {            
            List<int> refColumnID = new List<int>();           

            if (currentSetting.number2Text != null)
            {
                for (int x = 0; x < currentSetting.number2Text.Count; x++)
                {
                    if (currentTable.upperColumnName2ID.ContainsKey(currentSetting.number2Text[x].ToUpper()))
                        refColumnID.Add(currentTable.upperColumnName2ID[currentSetting.number2Text[x].ToUpper()]);
                }
            }


            Dictionary<int, int> refColumnID2AppendID = new Dictionary<int, int>();

            for (int x = 0; x < refColumnID.Count; x++)
                refColumnID2AppendID.Add(refColumnID[x], currentTable.columnName.Count + x);

            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();

            for (int x = 0; x < refColumnID.Count; x++)
            {
                factTable.Add(refColumnID2AppendID[refColumnID[x]], new List<double>());                
                key2Value.Add(refColumnID2AppendID[refColumnID[x]], new Dictionary<double, string>());
                value2Key.Add(refColumnID2AppendID[refColumnID[x]], new Dictionary<string, double>());
            }

            ConcurrentDictionary<int, number2Text> writeColumnThread = new ConcurrentDictionary<int, number2Text>();

            for (int worker = 0; worker < refColumnID.Count; worker++)
                writeColumnThread.TryAdd(worker, new number2Text());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };          

            Parallel.For(0, refColumnID.Count, options, x =>
            {
                (factTable[refColumnID2AppendID[refColumnID[x]]], key2Value[refColumnID2AppendID[refColumnID[x]]], value2Key[refColumnID2AppendID[refColumnID[x]]]) = calcNumber2Text(refColumnID[x], checkThreadCompleted, currentTable, refColumnID, refColumnID2AppendID);
            });

            do
            {
                Thread.Sleep(2);                

            } while (checkThreadCompleted.Count < refColumnID.Count);

            // upperColumnName2ID.Clear();

            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();

            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();
           
            for (int i = 0; i < currentTable.columnName.Count; i++)
            {              
                resultDataType.Add(i, currentTable.dataType[i]);
                resultColumnName.Add(i, currentTable.columnName[i].Trim());
                resultUpperColumnName2ID.Add(currentTable.columnName[i].Trim().ToUpper(), i);
                resultFactTable.Add(i, currentTable.factTable[i]);

                if (currentTable.dataType[i] != "Number")
                {
                    resultKey2Value.Add(i, currentTable.key2Value[i]);
                    resultValue2Key.Add(i, currentTable.value2Key[i]);
                }
            }          

            for (int x = 0; x < refColumnID.Count; x++)
            {
                var appendColumnID = refColumnID2AppendID[refColumnID[x]];
                resultDataType.Add(appendColumnID, "Text");
                resultColumnName.Add(appendColumnID, "Text:" + currentTable.columnName[refColumnID[x]]);
                resultUpperColumnName2ID.Add("TEXT:" + currentTable.columnName[refColumnID[x]].Trim().ToUpper(), appendColumnID);
                resultFactTable.Add(appendColumnID, factTable[appendColumnID]);
                resultKey2Value.Add(appendColumnID, key2Value[appendColumnID]);
                resultValue2Key.Add(appendColumnID, value2Key[appendColumnID]);
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
        public (List<double> factTable, Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) calcNumber2Text(int x, ConcurrentQueue<int> checkThreadCompleted, LedgerRAM currentTable, List<int> refColumnID, Dictionary<int, int> refColumnID2AppendID)
        {
            List<double> factTable = new List<double>();
            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();
            double count;          

            factTable.Add(refColumnID2AppendID[x]);

            for (int y = 1; y < currentTable.factTable[0].Count; y++)
            {
                
                string text =currentTable.factTable[x][y].ToString();               

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
            checkThreadCompleted.Enqueue(x);
            return (factTable, key2Value, value2Key);
        }
    }

}
