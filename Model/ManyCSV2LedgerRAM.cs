using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class manyCSV2LedgerRAMsetting
    {
        public int fileThread = 100;
        public string folderPath { get; set; }
        public string fileFilter { get; set; }
        public string subDirectory { get; set; }
        public string tableType { get; set; }
    }

    public class manyCSV2LedgerRAM
    {
        public LedgerRAM manyCSV2LedgerRAMProcess(Dictionary<string, LedgerRAM> ramStore, manyCSV2LedgerRAMsetting currentSetting)
        {
            fileList2LedgerRAM newFileList2LedgerRAM = new fileList2LedgerRAM();
            fileList2LedgerRAMsetting setFileList2LedgerRAM = new fileList2LedgerRAMsetting();
            setFileList2LedgerRAM.folderPath = currentSetting.folderPath;
            setFileList2LedgerRAM.fileFilter = currentSetting.fileFilter;
            setFileList2LedgerRAM.subDirectory = currentSetting.subDirectory;

            reverseCrosstab newReverseCrosstab = new reverseCrosstab();
            reverseCrosstabSetting setReverseCrosstab = new reverseCrosstabSetting();

            List<string> tableName = new List<string>();
            LedgerRAM currentOutput = newFileList2LedgerRAM.fileList2LedgerRAMProcess(setFileList2LedgerRAM);          

            LedgerRAM currentProcess = new LedgerRAM();
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
            setCSV2LedgerRAM.filePath = currentOutput.key2Value[1][currentOutput.factTable[1][1]];

            LedgerRAM tempTable = new LedgerRAM();

            if (currentSetting.tableType == "Crosstab")
            {
                tempTable = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                ramStore["InternalTable1"] = newReverseCrosstab.reverseCrosstabProcess(tempTable, setReverseCrosstab);
            }
            else
                ramStore["InternalTable1"] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);

            tableName.Add("InternalTable1");

            setCSV2LedgerRAM.commonTable = "InternalTable1";

            string message = Environment.NewLine + "       " + currentOutput.key2Value[1][currentOutput.factTable[1][1]] + " (Column:" + string.Format("{0:#,0}", ramStore["InternalTable1"].factTable.Count) + ", Row:" + string.Format("{0:#,0}", ramStore["InternalTable1"].factTable[0].Count) + ")"; ;
            Console.WriteLine(message);
            File.AppendAllText("Output\\log.txt", message + Environment.NewLine);

            bool isAllColumnMatch = true;           

            for (int y = 2; y < currentOutput.factTable[1].Count; y++)
            {
                setCSV2LedgerRAM.filePath = currentOutput.key2Value[1][currentOutput.factTable[1][y]];
                tableName.Add("InternalTable" + y.ToString());

                if (currentSetting.tableType == "Crosstab")
                {
                    tempTable = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                    ramStore["InternalTable" + y.ToString()] = newReverseCrosstab.reverseCrosstabProcess(tempTable, setReverseCrosstab);
                }
                else
                    ramStore["InternalTable" + y.ToString()] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);

                message = "       " + currentOutput.key2Value[1][currentOutput.factTable[1][y]] + " (Column:" + string.Format("{0:#,0}", ramStore["InternalTable" + y.ToString()].factTable.Count) + ", Row:" + string.Format("{0:#,0}", ramStore["InternalTable" + y.ToString()].factTable[0].Count) + ")";
                Console.WriteLine(message);
                File.AppendAllText("Output\\log.txt", message + Environment.NewLine);

                for (int x = 0; x < ramStore["InternalTable1"].columnName.Count; x++)
                {
                    if (ramStore["InternalTable" + y.ToString()].columnName.ContainsKey(x))
                    {
                        if (ramStore["InternalTable1"].columnName[x].ToUpper() != ramStore["InternalTable" + y.ToString()].columnName[x].ToUpper())                      
                            isAllColumnMatch = false;

                        if (ramStore["InternalTable1"].dataType[x] != ramStore["InternalTable" + y.ToString()].dataType[x])
                            isAllColumnMatch = false;

                    }
                    else                    
                        isAllColumnMatch = false;
                }               

                if (isAllColumnMatch == false)
                    break;
            }          

            mergeTable newMergeTable = new mergeTable();
            mergeTableSetting setMergeTable = new mergeTableSetting();
            setMergeTable.tableName = tableName;
            LedgerRAM mergedTable = new LedgerRAM();

            if (isAllColumnMatch == true)
                mergedTable = newMergeTable.mergeCommonTableProcess(ramStore, setMergeTable);            
            else
            {
                mergedTable = null;
                message = Environment.NewLine + "       " + "Columns of the above tables are not identical, tables are required to combine by alternative method." + Environment.NewLine;
                Console.WriteLine(message);
                File.AppendAllText("Output\\log.txt",  message + Environment.NewLine);
            }

            return mergedTable;          
        }

        public LedgerRAM manyDifferentCSV2LedgerRAMProcess(Dictionary<string, LedgerRAM> ramStore, manyCSV2LedgerRAMsetting currentSetting)
        {
            fileList2LedgerRAM newFileList2LedgerRAM = new fileList2LedgerRAM();
            fileList2LedgerRAMsetting setFileList2LedgerRAM = new fileList2LedgerRAMsetting();
            setFileList2LedgerRAM.folderPath = currentSetting.folderPath;
            setFileList2LedgerRAM.fileFilter = currentSetting.fileFilter;
            setFileList2LedgerRAM.subDirectory = currentSetting.subDirectory;

            List<string> tableName = new List<string>();
            LedgerRAM currentOutput = newFileList2LedgerRAM.fileList2LedgerRAMProcess(setFileList2LedgerRAM);
            LedgerRAM currentProcess = new LedgerRAM();
            csv2LedgerRAMSetting setCSV2LedgerRAM = new csv2LedgerRAMSetting();
          
            string message;

            for (int y = 1; y < currentOutput.factTable[1].Count; y++)
            {
                setCSV2LedgerRAM.filePath = currentOutput.key2Value[1][currentOutput.factTable[1][y]];
                tableName.Add("InternalTable" + y.ToString());
                ramStore["InternalTable" + y.ToString()] = currentProcess.csv2LedgerRAM(ramStore, setCSV2LedgerRAM);
                message = "       " + currentOutput.key2Value[1][currentOutput.factTable[1][y]] + " (Column:" + string.Format("{0:#,0}", ramStore["InternalTable" + y.ToString()].factTable.Count) + ", Row:" + string.Format("{0:#,0}", ramStore["InternalTable" + y.ToString()].factTable[0].Count) + ")";
                Console.WriteLine(message);
                File.AppendAllText("Output\\log.txt", message + Environment.NewLine);              
            }

            mergeTable newMergeTable = new mergeTable();
            mergeTableSetting setMergeTable = new mergeTableSetting();
            setMergeTable.tableName = tableName;
            LedgerRAM mergedTable = new LedgerRAM();
           
            mergedTable = newMergeTable.mergeTableProcess(ramStore, setMergeTable);          

            return mergedTable;
        }
    }
}