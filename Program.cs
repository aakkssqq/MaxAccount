using System;
using System.Collections.Generic;
using System.IO;

namespace MaxAccount 
{
    class Program
    {
        static void Main(string[] args)
        {  
            DateTime startTime = DateTime.Now;           
            bool isProcessEnd;

            if (!Directory.Exists("Output" + "\\"))
                Directory.CreateDirectory("Output" + "\\");           
            
            ruleProcessorSetting currentSetting = new ruleProcessorSetting();

            if (args.Length > 0)
            {
                currentSetting.calcRule = args[0];

                if (!args[0].Contains(".txt") && !args[0].Contains(".TXT"))
                    currentSetting.calcRule = args[0] + ".txt";
            }
            else
                currentSetting.calcRule = "SQL_CrosstabSQLTable.txt";

            List<string> newRule = new List<string>();

            if (args.Length <= 1)
            {
                startProcess(args);               
                Console.ReadLine();
            }
            else if (args[1].ToUpper() == "ENTER")
            {
                startProcess(args);              
            }
            else if (args.Length >= 2)
            {
                for (int i = 1; i < args.Length; i++)
                    newRule.Add(args[i]);

                startProcess(args);
            }

            void startProcess(string[] consoleArgs)
            {
                R2R currentRule = new R2R();
                isProcessEnd = currentRule.R2Rprocessing(currentSetting, newRule);                              
            }
        }
    }
}


