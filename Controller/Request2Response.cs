using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace MaxAccount
{
    public class R2Rsetting
    {


    }

    public class R2R
    {
        public bool R2Rprocessing(ruleProcessorSetting currentSetting, List<string> newRule)
        {          
            DateTime processStarted = DateTime.Now;   
            ConcurrentQueue<string> processMessage = new ConcurrentQueue<string>();
            string queueMessage = null;
            string message = null;
            bool isEnableMessage2Screen = true;
            bool isEnableMessage2File = true;

            Thread taskMessage = new Thread(monitorProcessMessage);
            taskMessage.Start();           

            message = Environment.NewLine + "Process Started @ " + processStarted + Environment.NewLine + Environment.NewLine;
            processMessage.Enqueue(message);

            void monitorProcessMessage()
            {
                while(true)
                {
                    if (processMessage.Count < 3)
                        Thread.Sleep(10);

                    if (processMessage.Count > 0)
                    {
                        processMessage.TryDequeue(out queueMessage);

                        if (queueMessage.ToUpper().Contains("DISABLE") && queueMessage.ToUpper().Contains("MESSAGE2SCREEN"))
                            isEnableMessage2Screen = false;

                        else if (queueMessage.ToUpper().Contains("ENABLE") && queueMessage.ToUpper().Contains("MESSAGE2SCREEN"))
                            isEnableMessage2Screen = true;

                        if (queueMessage.ToUpper().Contains("DISABLE") && queueMessage.ToUpper().Contains("MESSAGE2FILE"))
                            isEnableMessage2File = false;

                        else if (queueMessage.ToUpper().Contains("ENABLE") && queueMessage.ToUpper().Contains("MESSAGE2FILE"))
                            isEnableMessage2File = true;

                        if (isEnableMessage2Screen == true && !queueMessage.ToUpper().Contains("ENABLE") && !queueMessage.ToUpper().Contains("MESSAGE2SCREEN"))
                            Console.Write(queueMessage);

                        if (isEnableMessage2File == true && !queueMessage.ToUpper().Contains("ENABLE") && !queueMessage.ToUpper().Contains("MESSAGE2FILE"))
                            File.AppendAllText("Output\\log.txt", queueMessage);

                        if (queueMessage.ToUpper().Contains("PROCESS COMPLETED"))
                        {
                            var processCompleted = DateTime.Now;
                            message = string.Format(" @ " + processCompleted + "  Duration = {0:0.000}", (processCompleted - processStarted).TotalSeconds) + "s" + Environment.NewLine;
                            finalMessage();
                            break;
                        }

                        if (queueMessage.ToUpper().Contains("EXIT = Y"))
                        {
                            var processCompleted = DateTime.Now;
                            message = string.Format(Environment.NewLine + "Process Exit @ " + processCompleted + "  Duration = {0:0.000}", (processCompleted - processStarted).TotalSeconds) + "s" + Environment.NewLine;
                            finalMessage();
                            break;
                        }

                        if (queueMessage.ToUpper().Contains("ERROR"))
                        {
                            var processCompleted = DateTime.Now;
                            message = string.Format(Environment.NewLine + "Process Terminated by Error @ " + processCompleted + "  Duration = {0:0.000}", (processCompleted - processStarted).TotalSeconds) + "s" + Environment.NewLine;
                            finalMessage();
                            break;
                        }
                    }
                } 
            }

            void finalMessage()
            {
                Console.Write(message);
                File.AppendAllText("Output\\log.txt", message);
                message = Environment.NewLine + "******************************************************************************************************" + Environment.NewLine + Environment.NewLine;
                Console.Write(message);
                File.AppendAllText("Output\\log.txt", message);
            }

            ruleProcessing currentRule = new ruleProcessing();

            bool isEndProcess = currentRule.processRule(currentSetting, newRule, processMessage);                     

            return isEndProcess;
        }
    }
}
