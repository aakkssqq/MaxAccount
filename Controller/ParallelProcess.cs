using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace MaxAccount
{
    public class parallelSetting
    {

    }

    public class parallel
    {
        public void runParallelFor(string nextBlock, int r, Dictionary<string, Dictionary<int, bool>> isParallelProcess, Dictionary<string, Action<string, int>> commandDict, ConcurrentQueue<int> checkThreadCompleted, Dictionary<string, Dictionary<int, string>> ruleType)
        {
            isParallelProcess[nextBlock][r] = true;
            commandDict[ruleType[nextBlock][r].ToUpper().Trim()](nextBlock, r);
            checkThreadCompleted.Enqueue(r);
        }
    }
}
