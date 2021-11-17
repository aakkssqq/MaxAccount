using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MaxAccount
{
    public class fileList2LedgerRAMsetting
    {        
        public int columnThread = 100;
        public string folderPath { get; set; }
        public string fileFilter { get; set; }
        public string subDirectory { get; set; }        
    }

    public class fileList2LedgerRAM
    {
        public LedgerRAM fileList2LedgerRAMProcess(fileList2LedgerRAMsetting currentSetting)
        {              
            Dictionary<int, string> resultColumnName = new Dictionary<int, string>();
            Dictionary<int, string> resultDataType = new Dictionary<int, string>();
            Dictionary<string, int> resultUpperColumnName2ID = new Dictionary<string, int>();
            Dictionary<int, List<double>> resultFactTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> resultKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> resultValue2Key = new Dictionary<int, Dictionary<string, double>>();

            resultColumnName.Add(0, "Source Folder");
            resultColumnName.Add(1, "Result File Path");
            resultColumnName.Add(2, "Result File Name");

            resultDataType.Add(0, "Text");
            resultDataType.Add(1, "Text");
            resultDataType.Add(2, "Text");

            resultUpperColumnName2ID.Add("SOURCE FOLDER", 0);
            resultUpperColumnName2ID.Add("RESULT FILE PATH", 1);
            resultUpperColumnName2ID.Add("RESILT FILE NAME", 2);

            resultFactTable.Add(0, new List<double>());
            resultFactTable.Add(1, new List<double>());
            resultFactTable.Add(2, new List<double>());
            resultFactTable[0].Add(0);
            resultFactTable[1].Add(1);
            resultFactTable[2].Add(2);

            resultKey2Value.Add(0, new Dictionary<double, string>());
            resultKey2Value.Add(1, new Dictionary<double, string>());
            resultKey2Value.Add(2, new Dictionary<double, string>());

            resultValue2Key.Add(0, new Dictionary<string, double>());
            resultValue2Key.Add(1, new Dictionary<string, double>());
            resultValue2Key.Add(2, new Dictionary<string, double>());
            
            string text;
            int count;
            StringBuilder cellValue = new StringBuilder();
            string folderPath = currentSetting.folderPath.Replace(((char)92).ToString(), ((char)92).ToString() + ((char)92).ToString());

            if (currentSetting.subDirectory.ToUpper() == "INCLUDE")
            {
                foreach (string fileList in Directory.EnumerateFiles(folderPath, currentSetting.fileFilter, SearchOption.AllDirectories))
                {                  
                    int index = fileList.IndexOf(@"\", folderPath.Length);
                    saveFileList2Table(fileList);
                }
            }
            else
            {               
                foreach (string fileList in Directory.EnumerateFiles(folderPath, currentSetting.fileFilter, SearchOption.TopDirectoryOnly))
                {
                    int index = fileList.IndexOf(@"\", folderPath.Length);                   
                    saveFileList2Table(fileList);
                }
            }

            void saveFileList2Table(string fileList)
            {
                text = fileList.Substring(0, folderPath.Length).Replace(((char)92).ToString() + ((char)92).ToString(), ((char)92).ToString());                

                if (text.Length == 0)
                    cellValue.Append("null");

                if (resultValue2Key[0].ContainsKey(text))
                    resultFactTable[0].Add(resultValue2Key[0][text]);

                else
                {
                    count = resultValue2Key[0].Count;
                    resultKey2Value[0].Add(count, text);
                    resultValue2Key[0].Add(text, count);
                    resultFactTable[0].Add(count);
                }

                text = fileList.Replace(((char)92).ToString() + ((char)92).ToString(), ((char)92).ToString());               

                if (text.Length == 0)
                    cellValue.Append("null");

                if (resultValue2Key[1].ContainsKey(text))
                    resultFactTable[1].Add(resultValue2Key[1][text]);

                else
                {
                    count = resultValue2Key[1].Count;
                    resultKey2Value[1].Add(count, text);
                    resultValue2Key[1].Add(text, count);
                    resultFactTable[1].Add(count);
                }

                text = fileList.Substring(folderPath.Length, fileList.Length - folderPath.Length);

                if (text.Length == 0)
                    cellValue.Append("null");

                if (resultValue2Key[2].ContainsKey(text))
                    resultFactTable[2].Add(resultValue2Key[2][text]);

                else
                {
                    count = resultValue2Key[2].Count;
                    resultKey2Value[2].Add(count, text);
                    resultValue2Key[2].Add(text, count);
                    resultFactTable[2].Add(count);
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
