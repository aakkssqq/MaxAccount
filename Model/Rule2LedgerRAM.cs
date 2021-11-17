using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class rule2LedgerRAMsetting
    {
        public int rowThread = 100;
        public string filePath { get; set; }        
    }

    public class rule2LedgerRAM
    {
        public LedgerRAM rule2LedgerRAMprocess(LedgerRAM currentTable, rule2LedgerRAMsetting currentSetting)
        {
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            resultColumnName.Add(0, "File Name");
            resultColumnName.Add(1, "Block");
            resultColumnName.Add(2, "Rule Type");
            resultColumnName.Add(3, "Rule Detail");

            resultDataType.Add(0, "Text");
            resultDataType.Add(1, "Text");
            resultDataType.Add(2, "Text");
            resultDataType.Add(3, "Text");

            resultUpperColumnName2ID.Add("FILE NAME", 0);
            resultUpperColumnName2ID.Add("BLOCK", 1);
            resultUpperColumnName2ID.Add("RULE TYPE", 2);
            resultUpperColumnName2ID.Add("RULE DETAIL", 3);

            resultFactTable.Add(0, new List<double>());
            resultFactTable.Add(1, new List<double>());
            resultFactTable.Add(2, new List<double>());
            resultFactTable.Add(3, new List<double>());
            resultFactTable[0].Add(0);
            resultFactTable[1].Add(1);
            resultFactTable[2].Add(2);
            resultFactTable[3].Add(3);

            resultKey2Value.Add(0, new Dictionary<double, string>());
            resultKey2Value.Add(1, new Dictionary<double, string>());
            resultKey2Value.Add(2, new Dictionary<double, string>());
            resultKey2Value.Add(3, new Dictionary<double, string>());

            resultValue2Key.Add(0, new Dictionary<string, double>());
            resultValue2Key.Add(1, new Dictionary<string, double>());
            resultValue2Key.Add(2, new Dictionary<string, double>());
            resultValue2Key.Add(3, new Dictionary<string, double>());

            string text;
            int count;
            StringBuilder cellValue = new StringBuilder();

            /*
            StringBuilder doubleQuote1 = new StringBuilder();
            doubleQuote1.Append((char)34);

            StringBuilder doubleQuote2 = new StringBuilder();
            doubleQuote2.Append((char)34);
            doubleQuote2.Append((char)34);
            */

            int columnID = upperColumnName2ID[currentSetting.filePath.ToUpper()];
            StringBuilder block = new StringBuilder();
            StringBuilder ruleType = new StringBuilder();
            StringBuilder ruleDetail = new StringBuilder();            
            bool isRuleType = true;
            bool isRuleDetail = false;
            bool isBlock = false;
            string currentBlock;          

            for (int y = 1; y < currentTable.factTable[columnID].Count; y++)
            {

                var filePath = currentTable.key2Value[columnID][currentTable.factTable[columnID][y]].ToString();

                int index = filePath.LastIndexOf(@"\");

                var file = filePath.Substring(index + 1, filePath.Length - index - 1);

                currentBlock = "Main";

                byte[] ruleBytestream = File.ReadAllBytes(filePath.Replace(((char)92).ToString(), ((char)92).ToString() + ((char)92).ToString()));                

                for (int i = 0; i < ruleBytestream.Length; i++)
                {
                    if (ruleBytestream[i] == 35)                   
                        isBlock = true;                    

                    if(ruleBytestream[i] != 35 && isBlock == true)
                        block.Append((char)ruleBytestream[i]);

                    if (ruleBytestream[i] != 123 && isRuleType == true && isBlock == false && isRuleDetail == false)
                        ruleType.Append((char)ruleBytestream[i]);

                    if (ruleBytestream[i] == 123)
                    {
                        isRuleType = false;
                        isRuleDetail = true;
                    }

                    if (ruleBytestream[i] == 125)                    
                        isRuleDetail = false;                   

                    if(ruleBytestream[i] != 123 && isRuleDetail == true)
                        ruleDetail.Append((char)ruleBytestream[i]);

                    if (ruleBytestream[i] == 10 || ruleBytestream[i] == 13)
                    {
                        isBlock = false;
                        isRuleType = true;
                        isRuleDetail = false;

                        if (block.ToString().Trim().Length > 0 || ruleType.ToString().Trim().Length > 0 || ruleDetail.ToString().Trim().Length > 0)
                        {
                            if (block.ToString().Trim().Length > 0)
                                currentBlock = block.ToString().Trim();

                            if (ruleType.ToString().Trim().Length > 0)
                            {
                                text = file;
                               
                                if (resultValue2Key[0].ContainsKey(text))
                                    resultFactTable[0].Add(resultValue2Key[0][text]);

                                else
                                {
                                    count = resultValue2Key[0].Count;
                                    resultKey2Value[0].Add(count, text);
                                    resultValue2Key[0].Add(text, count);
                                    resultFactTable[0].Add(count);
                                }

                                text = currentBlock;

                                if (resultValue2Key[1].ContainsKey(text))
                                    resultFactTable[1].Add(resultValue2Key[1][text]);

                                else
                                {
                                    count = resultValue2Key[1].Count;
                                    resultKey2Value[1].Add(count, text);
                                    resultValue2Key[1].Add(text, count);
                                    resultFactTable[1].Add(count);
                                }

                                text = ruleType.ToString().Trim();                               

                                if (resultValue2Key[2].ContainsKey(text))
                                    resultFactTable[2].Add(resultValue2Key[2][text]);

                                else
                                {
                                    count = resultValue2Key[2].Count;
                                    resultKey2Value[2].Add(count, text);
                                    resultValue2Key[2].Add(text, count);
                                    resultFactTable[2].Add(count);
                                }

                                text = ruleDetail.ToString().Trim();                                                               
                               
                                if (text.Length == 0)
                                    cellValue.Append("null");

                                if (resultValue2Key[3].ContainsKey(text))
                                    resultFactTable[3].Add(resultValue2Key[3][text]);

                                else
                                {
                                    count = resultValue2Key[3].Count;
                                    resultKey2Value[3].Add(count, text);
                                    resultValue2Key[3].Add(text, count);
                                    resultFactTable[3].Add(count);
                                }
                            }
                        }

                        block.Clear();
                        ruleType.Clear();
                        ruleDetail.Clear();
                    }
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
    }
}
