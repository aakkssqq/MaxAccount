using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class condition2ExactSetting
    {
        public int rowThread = 100;
        public string filterType { get; set; }
        public List<string> selectedColumnName { get; set; }
        public Dictionary<int, List<string>> compareOperator { get; set; }
        public Dictionary<int, List<string>> selectedText { get; set; }        
    }
    public class condition2Exact
    {
        public Dictionary<int, Dictionary<double, string>> condition2ExactProcess(LedgerRAM currentTable, condition2ExactSetting currentSetting)
        {
            Dictionary<int, Dictionary<double, string>> matchedKey = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, bool> isStatisifyCondition = new Dictionary<int, bool>();

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<int> selectedColumnID = new List<int>();

            for (int i = 0; i < currentSetting.selectedColumnName.Count; i++)
                if (upperColumnName2ID.ContainsKey(currentSetting.selectedColumnName[i].ToUpper()))
                    selectedColumnID.Add(upperColumnName2ID[currentSetting.selectedColumnName[i].ToUpper()]);

            List<bool> isAllCompareOperatorUseEqual = new List<bool>();

            for (int x = 0; x < selectedColumnID.Count; x++)
            {
                isAllCompareOperatorUseEqual.Add(true);
                for (int z = 0; z < currentSetting.compareOperator[x].Count; z++)
                {
                    if (currentSetting.compareOperator[x][z] != "=")
                        isAllCompareOperatorUseEqual[x] = false;
                }
            } 
            
            if (currentSetting.filterType == "And")
            {               
                for (int x = 0; x < selectedColumnID.Count; x++)
                {
                    if (currentTable.dataType[selectedColumnID[x]] != "Number")
                    {
                        matchedKey.Add(x, new Dictionary<double, string>());

                        for (double y = 0; y < currentTable.key2Value[selectedColumnID[x]].Count; y++)
                        {
                            isStatisifyCondition[x] = true;

                            for (int z = 0; z < currentSetting.selectedText[x].Count; z++)
                            {
                                if (currentSetting.compareOperator[x][z] == ">=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) < 0)
                                        isStatisifyCondition[x] = false;

                                if (currentSetting.compareOperator[x][z] == ">")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) <= 0)
                                        isStatisifyCondition[x] = false;

                                if (currentSetting.compareOperator[x][z] == "<=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) > 0)
                                        isStatisifyCondition[x] = false;

                                if (currentSetting.compareOperator[x][z] == "<")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) >= 0)
                                        isStatisifyCondition[x] = false;

                                if (currentSetting.compareOperator[x][z] == "!=")
                                {
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) == 0)                                   
                                       if (!matchedKey[x].ContainsKey(y))
                                            isStatisifyCondition[x] = false;                                  
                                }

                                if (isAllCompareOperatorUseEqual[x] == true)
                                    isStatisifyCondition[x] = false;

                                if (currentSetting.compareOperator[x][z] == "=")
                                {                                   
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) == 0)
                                    {
                                        if (!matchedKey[x].ContainsKey(y))
                                            isStatisifyCondition[x] = false;
                                    }

                                    if (currentSetting.selectedText[x][z] == "*")                                  
                                        isStatisifyCondition[x] = true;
                                }
                            }

                            if (isStatisifyCondition[x] == true)
                                if (!matchedKey[x].ContainsKey(y))
                                    matchedKey[x].Add(y, currentTable.key2Value[selectedColumnID[x]][y]);                              
                            
                            for (int z = 0; z < currentSetting.selectedText[x].Count; z++)
                            {
                                if (currentSetting.compareOperator[x][z] == "=" && currentSetting.selectedText[x][z] != "*")
                                {
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) == 0)
                                    {
                                        if (!matchedKey[x].ContainsKey(y))
                                            matchedKey[x].Add(y, currentTable.key2Value[selectedColumnID[x]][y]);                                        
                                    }
                                    else
                                        isStatisifyCondition[x] = false;
                                }
                            }                            
                        }
                    }
                }
            }

            if (currentSetting.filterType == "Or")
            {
                for (int x = 0; x < selectedColumnID.Count; x++)
                {
                    if (currentTable.dataType[selectedColumnID[x]] != "Number")
                    {
                        matchedKey.Add(x, new Dictionary<double, string>());

                        for (double y = 1; y < currentTable.key2Value[selectedColumnID[x]].Count; y++)
                        {
                            isStatisifyCondition[x] = false;

                            for (int z = 0; z < currentSetting.selectedText[x].Count; z++)
                            {
                                if (currentSetting.compareOperator[x][z] == ">=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) >= 0)
                                        isStatisifyCondition[x] = true;

                                if (currentSetting.compareOperator[x][z] == ">")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) > 0)
                                        isStatisifyCondition[x] = true;

                                if (currentSetting.compareOperator[x][z] == "<=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) <= 0)
                                        isStatisifyCondition[x] = true;

                                if (currentSetting.compareOperator[x][z] == "<")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) < 0)
                                        isStatisifyCondition[x] = true;

                                if (currentSetting.compareOperator[x][z] == "=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) == 0)
                                        isStatisifyCondition[x] = true;

                                if (currentSetting.compareOperator[x][z] == "!=")
                                    if (string.Compare(currentTable.key2Value[selectedColumnID[x]][y].ToString(), currentSetting.selectedText[x][z]) == 0)
                                        isStatisifyCondition[x] = false;
                            }

                            if (isStatisifyCondition[x] == true)
                                if (!matchedKey[x].ContainsKey(y))
                                    matchedKey[x].Add(y, currentTable.key2Value[selectedColumnID[x]][y]);
                        }
                    }
                }
            }
            return matchedKey;
        } 
    }
}
