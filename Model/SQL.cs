using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace MaxAccount
{
    public class SQLsetting
    {
        public string sqlStatement { get; set; }
        public int sentSQLstatement { get; set; }
        public string currentSQLServer { get; set; }        
        public string currentConnectionString { get; set; }
        public string sourceTable { get; set; }
        public string resultTable { get; set; }
        public string resultDatabase { get; set; }
        public List<string> resultColumn { get; set; }
        public string updateSQLtype { get; set; }
        public string filterType { get; set; }
        public string command { get; set; }
        public List<string> selectedColumnName { get; set; }
        public Dictionary<int, List<string>> compareOperator { get; set; }
        public Dictionary<int, List<string>> selectedText { get; set; }
        public Dictionary<int, List<double>> selectedNumber { get; set; }
        public Dictionary<int, List<string>> selectedTextNumber { get; set; }
        public List<string> groupByColumnName { get; set; }
        public List<string> aggregateFunction { get; set; }
        public List<string> aggregateByColumnName { get; set; }        

        public int maxSQLstatementSize = 1000;

        public int timeOut = 600;

        public int rowThread = 1;
    }

    public class SQL
    {
        public string createDatabase(SQLsetting currentSetting)
        {
            string str;
            string message = null;          
           
            SqlConnection myConn = new SqlConnection(currentSetting.currentConnectionString);

            str = "CREATE DATABASE " + currentSetting.resultDatabase;

            SqlCommand myCommand = new SqlCommand(str, myConn);

            try
            {
                myConn.Open();
                myCommand.ExecuteNonQuery();
                message = "SQL server database (" + currentSetting.resultDatabase + "} is created successfully";
            }
            catch (System.Exception)
            {
                message = "Fail to create the SQL server database";
            }
            finally
            {
                if (myConn.State == ConnectionState.Open)
                {
                    myConn.Close();
                }
            }

            return message;
        }
        public string removeDatabase(SQLsetting currentSetting)
        {
            string str;
            string message = null;

            SqlConnection myConn = new SqlConnection(currentSetting.currentConnectionString);

            str = "DROP DATABASE " + currentSetting.resultDatabase + ";";            

            SqlCommand myCommand = new SqlCommand(str, myConn);

            try
            {
                myConn.Open();
                myCommand.ExecuteNonQuery();
                message = "The SQL database (" + currentSetting.resultDatabase + "} is removed successfully";
            }
            catch (System.Exception)
            {
                message = "Fail to remove the SQL server database";
            }
            finally
            {
                if (myConn.State == ConnectionState.Open)
                {
                    myConn.Close();
                }
            }

            return message;
        }
        public string removeTable(SQLsetting currentSetting)
        {
            string str;
            string message = null;

            SqlConnection myConn = new SqlConnection(currentSetting.currentConnectionString);

            str = "DROP TABLE " + currentSetting.resultTable + ";";

            SqlCommand myCommand = new SqlCommand(str, myConn);

            try
            {
                myConn.Open();
                myCommand.ExecuteNonQuery();
                message = "The SQL Table (" + currentSetting.resultDatabase + "} is removed successfully";
            }
            catch (System.Exception)
            {
                message = "Fail to remove the SQL Table";
            }
            finally
            {
                if (myConn.State == ConnectionState.Open)
                {
                    myConn.Close();
                }
            }

            return message;
        }
        public string removeColumn(SQLsetting currentSetting)
        {          
            string message = null;
            StringBuilder sqlStatement = new StringBuilder();


            SqlConnection myConn = new SqlConnection(currentSetting.currentConnectionString);

            sqlStatement.Append("ALTER TABLE " + currentSetting.resultTable + " DROP COLUMN ");           

            for (int x = 0; x < currentSetting.resultColumn.Count; x++)
            {
                if(x < currentSetting.resultColumn.Count - 1)
                    sqlStatement.Append(currentSetting.resultColumn[x] + ",");
                else
                    sqlStatement.Append(currentSetting.resultColumn[x] + ";");
            }

            SqlCommand myCommand = new SqlCommand(sqlStatement.ToString(), myConn);

            try
            {
                myConn.Open();
                myCommand.ExecuteNonQuery();
                message = "The SQL Column (" + currentSetting.resultColumn + "} is removed successfully";
            }
            catch (System.Exception)
            {
                message = "Fail to remove the SQL Column";
            }
            finally
            {
                if (myConn.State == ConnectionState.Open)
                {
                    myConn.Close();
                }
            }

            return message;
        }
        public string LedgerRAM2SQL(LedgerRAM currentTable, SQLsetting currentSetting)
        {  
            string classMessage;
            string message = "Fail";
            bool isExecute = true;

            using (SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString))
            {
                try
                {
                    dbConnection.Open();
                }

                catch (Exception)
                {
                    message = "Fail to connect the SQL server";
                    isExecute = false;
                }

                DataTable testDataTable = new DataTable();
                List<string> columnOrder = new List<string>();
                List<string> columnDataType = new List<string>();
                List<string> unmatchColumn = new List<string>();
                List<string> missingColumn = new List<string>();

                try
                {
                    if (currentSetting.updateSQLtype == "LEDGERRAMAPPEND2SQLSERVER")
                    {
                        currentSetting.sqlStatement = "Select top 10 * from " + currentSetting.resultTable;
                        (testDataTable, classMessage) = SQL2DataTable(currentSetting);

                        for (int x = 0; x < testDataTable.Columns.Count; x++)
                        {
                            columnOrder.Add(testDataTable.Columns[x].ColumnName);
                            columnDataType.Add(testDataTable.Columns[x].DataType.ToString());                          
                        }

                        if (testDataTable.Rows.Count > 0)
                        {
                            List<string> revisedColumnName = new List<string>();
                            Dictionary<string, int> upperRevisedColumnName = new Dictionary<string, int>();

                            for (int x = 0; x < columnOrder.Count; x++)
                            {
                                if (columnOrder[x].Contains("_"))
                                    revisedColumnName.Add(columnOrder[x].Replace("_", " ").Trim());

                                else if (columnOrder[x] == "DC")
                                    revisedColumnName.Add("D/C");

                                else
                                    revisedColumnName.Add(columnOrder[x].Trim());
                            }

                            for (int x = 0; x < revisedColumnName.Count; x++)
                            {
                                if (!currentTable.upperColumnName2ID.ContainsKey(revisedColumnName[x].ToUpper()))
                                    unmatchColumn.Add(revisedColumnName[x]);

                                upperRevisedColumnName.Add(revisedColumnName[x].ToUpper(), x);
                            }


                            for (int x = 0; x < currentTable.columnName.Count; x++)                          
                                if (!upperRevisedColumnName.ContainsKey(currentTable.columnName[x].ToUpper()))
                                    missingColumn.Add(currentTable.columnName[x]);

                            if (unmatchColumn.Count == 0 && missingColumn.Count == 0)
                            {
                                for (int x = 0; x < currentTable.columnName.Count; x++)
                                {
                                    selectColumn newSelectColumn = new selectColumn();
                                    selectColumnSetting setSelectColumn = new selectColumnSetting();
                                    setSelectColumn.selectColumn = revisedColumnName;
                                    setSelectColumn.selectType = "Add";

                                    currentTable = newSelectColumn.selectColumnName(currentTable, setSelectColumn);                                   
                                }
                            }
                            else
                            {
                                message = "Fail, " + unmatchColumn.Count + " unmatched column name found, " + missingColumn.Count + " missing column name found";
                                isExecute = false;
                            }
                        }
                    }

                    dbConnection.Close();
                }

                catch (Exception)
                {
                    message = "Fail to get data from the table for validation";
                    isExecute = false;
                }
            }           

            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            for (int x = 0; x < currentTable.columnName.Count; x++)
            {
                columnName.Add(x, currentTable.columnName[x].Replace("Year End:", "Year_End$").Replace(" ", "_").Replace("/", ""));
                upperColumnName2ID.Add(currentTable.columnName[x].Replace("YEAR END:", "YEAR_END$").Replace(" ", "_").Replace("/", "").ToUpper(), x);
            }

            currentTable.columnName = columnName;
            currentTable.upperColumnName2ID = upperColumnName2ID;

            LedgerRAM2DataTablesetting setLedgerRAM2DataTable = new LedgerRAM2DataTablesetting();
            LedgerRAM2DataTabledataFlow process2 = new LedgerRAM2DataTabledataFlow();
            DataTable DT = process2.LedgerRAM2DataTable(currentTable, setLedgerRAM2DataTable);           

            StringBuilder sql = new StringBuilder();

            if (currentSetting.updateSQLtype == "LEDGERRAMCLONE2SQLSERVER")            
                createTableSQL();
          
            using (SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString))
            {
                try
                {
                    dbConnection.Open();
                }

                catch (Exception)
                {
                    message = "Fail to connect the SQL server";
                    isExecute = false;
                }

                if (isExecute == true)
                {   
                    if (currentSetting.updateSQLtype == "LEDGERRAMCLONE2SQLSERVER")                   
                        executeSQL(dbConnection);

                    try
                    {
                        using (SqlBulkCopy SQLServer = new SqlBulkCopy(dbConnection))
                        {  
                            SQLServer.DestinationTableName = currentSetting.resultTable;
                            SQLServer.BulkCopyTimeout = currentSetting.timeOut;
                            SQLServer.WriteToServer(DT);                            
                        }
                    }

                    catch (Exception)
                    {
                        createTableSQL();
                        executeSQL(dbConnection);

                        if (isExecute == true)
                        {
                            using (SqlBulkCopy SQLServer = new SqlBulkCopy(dbConnection))
                            {
                                SQLServer.DestinationTableName = currentSetting.resultTable;
                                SQLServer.BulkCopyTimeout = currentSetting.timeOut;
                                SQLServer.WriteToServer(DT);
                            }
                        }
                    }

                    if (isExecute == true)
                    {
                        SqlCommand command3 = new SqlCommand("Select count(*) from " + currentSetting.resultTable, dbConnection);
                        command3.CommandTimeout = currentSetting.timeOut;
                        int rowCount = (int)command3.ExecuteScalar();

                        SqlCommand command4 = new SqlCommand("Select count(column_name)as Number from information_schema.columns where table_name = '" + currentSetting.resultTable + "'", dbConnection);
                        command4.CommandTimeout = currentSetting.timeOut;
                        int columnCount = (int)command4.ExecuteScalar();

                        dbConnection.Close();

                        message = currentSetting.resultTable + "(Column:" + string.Format("{0:#,0}", columnCount) + ", Row:" + string.Format("{0:#,0}", (rowCount + 1)) + ")";
                    }
                }
            }

            void createTableSQL()
            {
                sql.Append("CREATE TABLE " + currentSetting.resultTable + "(");

                for (int x = 0; x < currentTable.dataType.Count; x++)
                {
                    if (x < currentTable.dataType.Count - 1)
                    {

                        if (currentTable.dataType[x] == "Number")
                            sql.Append(" " + currentTable.columnName[x] + " " + "float,");
                        else if (currentTable.dataType[x] == "Date")
                            sql.Append(" " + currentTable.columnName[x] + " " + "datetime,");
                        else
                            sql.Append(" " + currentTable.columnName[x] + " " + "varchar(100),");
                    }
                    else
                    {
                        if (currentTable.dataType[x] == "Number")
                            sql.Append(" " + currentTable.columnName[x] + " " + "float);");
                        else if (currentTable.dataType[x] == "Date")
                            sql.Append(" " + currentTable.columnName[x] + " " + "datetime);");
                        else
                            sql.Append(" " + currentTable.columnName[x] + " " + "varchar(100));");
                    }
                }
            }

            void executeSQL(SqlConnection dbConnection)
            {
                try
                {
                    using (SqlCommand command2 = new SqlCommand(sql.ToString(), dbConnection))
                        command2.ExecuteNonQuery();
                }

                catch (Exception)
                {
                    if (currentSetting.updateSQLtype == "LEDGERRAMCLONE2SQLSERVER")
                    {
                      //  try
                        {
                            using (SqlCommand command = new SqlCommand("Drop table " + currentSetting.resultTable + "; ", dbConnection))
                            {                                
                                command.ExecuteNonQuery();
                            }
                        }
                      //  catch (Exception)
                        {

                        }

                        using (SqlCommand command2 = new SqlCommand(sql.ToString(), dbConnection))
                        {                            
                            command2.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        message = "Fail to update the table";
                        isExecute = false;
                    }
                }
            }


            return message;
        }       
        public string DataTable2SQL(DataTable currentTable, SQLsetting currentSetting)
        { 
            string message = "Fail";
            bool isExecute = true;
         
            for (int x = 0; x < currentTable.Columns.Count; x++)      
                currentTable.Columns[x].ColumnName = currentTable.Columns[x].ColumnName.Replace(" ", "_").Replace("/", "");

            StringBuilder sql = new StringBuilder();

            if (currentSetting.updateSQLtype == "DATATABLECLONE2SQLSERVER")
                createTableSQL();

            using (SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString))
            {
                try
                {
                    dbConnection.Open();
                }

                catch (Exception)
                {
                    message = "Fail to connect the SQL server";
                    isExecute = false;
                }

                if (isExecute == true)
                {
                    if (currentSetting.updateSQLtype == "DATATABLECLONE2SQLSERVER")
                        executeSQL(dbConnection);

                    try
                    {
                        using (SqlBulkCopy SQLServer = new SqlBulkCopy(dbConnection))
                        {
                            SQLServer.DestinationTableName = currentSetting.resultTable;
                            SQLServer.BulkCopyTimeout = currentSetting.timeOut;
                            SQLServer.WriteToServer(currentTable);
                        }
                    }

                    catch (Exception)
                    {
                        createTableSQL();
                        executeSQL(dbConnection);

                        if (isExecute == true)
                        {
                            using (SqlBulkCopy SQLServer = new SqlBulkCopy(dbConnection))
                            {
                                SQLServer.DestinationTableName = currentSetting.resultTable;
                                SQLServer.BulkCopyTimeout = currentSetting.timeOut;
                                SQLServer.WriteToServer(currentTable);
                            }
                        }
                    }

                    if (isExecute == true)
                    {
                        SqlCommand command3 = new SqlCommand("Select count(*) from " + currentSetting.resultTable, dbConnection);
                        int rowCount = (int)command3.ExecuteScalar();

                        SqlCommand command4 = new SqlCommand("Select count(column_name)as Number from information_schema.columns where table_name = '" + currentSetting.resultTable + "'", dbConnection);
                        int columnCount = (int)command4.ExecuteScalar();

                        dbConnection.Close();

                        message = currentSetting.resultTable + "(Column:" + string.Format("{0:#,0}", columnCount) + ", Row:" + string.Format("{0:#,0}", (rowCount + 1)) + ")";
                    }
                }
            }

            void createTableSQL()
            {
                sql.Append("CREATE TABLE " + currentSetting.resultTable + "(");

                for (int x = 0; x < currentTable.Columns.Count; x++)
                {
                    if (x < currentTable.Columns.Count - 1)
                    {

                        if (currentTable.Columns[x].DataType.Name.ToString() == "Double")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "float,");
                        else if (currentTable.Columns[x].DataType.Name.ToString() == "Decimal")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "decimal,");
                        else if (currentTable.Columns[x].DataType.Name.ToString() == "DateTime")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "datetime,");
                        else
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "varchar(100),");
                    }
                    else
                    {
                        if (currentTable.Columns[x].DataType.Name.ToString() == "Double")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "float);");
                        else if (currentTable.Columns[x].DataType.Name.ToString() == "Decimal")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "decimal);,");
                        else if (currentTable.Columns[x].DataType.Name.ToString() == "DateTime")
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "datetime);,");
                        else
                            sql.Append(" " + currentTable.Columns[x].ColumnName + " " + "varchar(100));,");
                    }
                }
            }

            void executeSQL(SqlConnection dbConnection)
            {
                try
                {
                    using (SqlCommand command2 = new SqlCommand(sql.ToString(), dbConnection))
                        command2.ExecuteNonQuery();
                }

                catch (Exception)
                {
                    if (currentSetting.updateSQLtype == "DATATABLECLONE2SQLSERVER")
                    {

                        using (SqlCommand command = new SqlCommand("Drop table " + currentSetting.resultTable + "; ", dbConnection))
                            command.ExecuteNonQuery();

                        using (SqlCommand command2 = new SqlCommand(sql.ToString(), dbConnection))
                            command2.ExecuteNonQuery();
                    }
                    else
                    {
                        message = "Fail to update the table";
                        isExecute = false;
                    }
                }
            }


            return message;
        }
        public (DataTable, string) SQL2DataTableFirstRecord(SQLsetting currentSetting)
        {
            string message = null;
            bool isExecute = true;

            DataTable currentOutput = new DataTable();
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);           

            if (isExecute == true)
            {
                SqlDataAdapter da = new SqlDataAdapter(sql);

                try
                {
                    da.Fill(currentOutput);
                    dbConnection.Close();
                    da.Dispose();
                    message = "Success";
                }

                catch (Exception)
                {
                    message = "Fail to select data from the table";
                }
            }

            return (currentOutput, message);
        }
        public (DataTable, string) SQL2DataTable(SQLsetting currentSetting)
        {
            string message = null;
            bool isExecute = true;

            DataTable currentOutput = new DataTable();
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);            
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);
           
            try
            {
                dbConnection.Open();
            }

            catch (Exception)
            {
                message = "Fail to connect the SQL server";
                isExecute = false;
            }

            if (isExecute == true)
            {
                SqlDataAdapter da = new SqlDataAdapter(sql);

                try
                {
                    da.Fill(currentOutput);
                    dbConnection.Close();
                    da.Dispose();
                    message = "Success";
                }

                catch (Exception)
                {
                    message = "Fail to select data from the table";
                }
            }

            return (currentOutput, message);
        }
        public (LedgerRAM, string) SQL2LedgerRAM(SQLsetting currentSetting)
        {
            string message = null;
            bool isExecute = true;           

            LedgerRAM dataTable2LedgerRAM = new LedgerRAM();
            DataTable currentOutput = new DataTable();
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);         

            try
            {
                dbConnection.Open();
            }

            catch (Exception)
            {
                message = "Fail to connect the SQL server";
                isExecute = false;
            }

            if (isExecute == true)
            {
                SqlDataAdapter da = new SqlDataAdapter(sql);

                try
                {
                    da.Fill(currentOutput);
                    dbConnection.Close();
                    da.Dispose();
                    message = "Success";
                }

                catch (Exception)
                {
                    message = "Fail to select data from the table";
                    isExecute = false;
                }
            }

            LedgerRAM finalOutput = new LedgerRAM();

            if (isExecute == true)
            {
                LedgerRAM currentProcess = new LedgerRAM();
                
                dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
                dataTable2LedgerRAM = currentProcess.dataTable2LedgerRAM(currentOutput, setDataTable2LedgerRAM);

                Dictionary<int, string> revisedColumnName = new Dictionary<int, string>();
                Dictionary<string, int> revisedUpperColumnName2ID = new Dictionary<string, int>();

                if (currentSetting.command != null)
                {                    
                    for (int x = 0; x < dataTable2LedgerRAM.columnName.Count; x++)
                    {
                        revisedColumnName.Add(x, dataTable2LedgerRAM.columnName[x].ToString().Replace("Max$", "Max:").Replace("Min$", "Min:"));
                        revisedUpperColumnName2ID.Add(dataTable2LedgerRAM.columnName[x].ToString().ToUpper().Replace("MAX$", "MAX:").Replace("MIN$", "MIN:"), x);
                    }

                    finalOutput.columnName = revisedColumnName;
                    finalOutput.upperColumnName2ID = revisedUpperColumnName2ID;
                    finalOutput.dataType = dataTable2LedgerRAM.dataType;
                    finalOutput.factTable = dataTable2LedgerRAM.factTable;
                    finalOutput.key2Value = dataTable2LedgerRAM.key2Value;
                    finalOutput.value2Key = dataTable2LedgerRAM.value2Key;                    
                }
                else              
                    finalOutput = dataTable2LedgerRAM;               
            }

            return (finalOutput, message);
        }
        public string runNonQuerySQL(SQLsetting currentSetting)
        {
            string message = null;
            bool isExecute = true;            
          
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);
            sql.CommandTimeout = currentSetting.timeOut;

            try
            {
                dbConnection.Open();
                isExecute = true;
            }

            catch (Exception)
            {
                message = "Fail to connect the SQL server";
                isExecute = false;
            }

            if (isExecute == true)
            {
                try
                {                   
                    sql.ExecuteNonQuery();
                    isExecute = true;
                }

                catch (Exception)
                {
                    message = "Fail to execute NonQuery SQL";
                    isExecute = false;

                }
            }

            return message;
        }        
        public string createSQLstatement(SQLsetting currentSetting)
        {
            StringBuilder sqlStatement = new StringBuilder();

            if (currentSetting.command == "FILTERSQLROW.ANDCONDITION" || currentSetting.command == "FILTERSQLROW.ORCONDITION")
                sqlStatement.Append(" Select * from " + currentSetting.sourceTable + " Where ");

            if (currentSetting.command == "REMOVESQLROW.ANDCONDITION" || currentSetting.command == "REMOVESQLROW.ORCONDITION")
                sqlStatement.Append(" Delete from " + currentSetting.sourceTable + " Where ");

            sqlStatement.Append(Environment.NewLine);

            for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
            {
                sqlStatement.Append("(");

                for (int y = 0; y < currentSetting.compareOperator[x].Count; y++)
                {
                    sqlStatement.Append(currentSetting.selectedColumnName[x].Replace(" ", "_").Replace("D/C", "DC") + " " + currentSetting.compareOperator[x][y] + "'" + currentSetting.selectedTextNumber[x][y] + "'");

                    if (y < currentSetting.compareOperator[x].Count - 1)
                    {
                        if (currentSetting.compareOperator[x][y] == "=" || currentSetting.compareOperator[x][y + 1] == "=")
                            sqlStatement.Append(" Or ");
                        else
                        {
                            if (currentSetting.filterType == "And")
                                sqlStatement.Append(" And ");

                            if (currentSetting.filterType == "Or")
                                sqlStatement.Append(" Or ");
                        }
                    }
                }

                sqlStatement.Append(")");

                if (x < currentSetting.selectedColumnName.Count - 1)
                {
                    if (currentSetting.filterType == "And")
                        sqlStatement.Append(" And ");

                    if (currentSetting.filterType == "Or")
                        sqlStatement.Append(" Or ");
                }
            }

            if (currentSetting.command == "FILTERSQLROW.ANDCONDITION" || currentSetting.command == "FILTERSQLROW.ORCONDITION" || currentSetting.command == "REMOVESQLROW.ANDCONDITION" || currentSetting.command == "REMOVESQLROW.ORCONDITION")
                sqlStatement.Append(";");

            return sqlStatement.ToString();
        }
        public (LedgerRAM, string) oneRowfilterSQL2LedgerRAM(SQLsetting currentSetting)
        {
            currentSetting.sqlStatement = createSQLstatement(currentSetting);

            return runSQL2LedgerRAM(currentSetting);
        }
        public (LedgerRAM, string) runSQL2LedgerRAM(SQLsetting currentSetting)
        {
            string message = null;
           
            bool isExecute = true;            
             
            LedgerRAM dataTable2LedgerRAM = new LedgerRAM();
          
            DataTable currentOutput = new DataTable();
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);
            sql.CommandTimeout = currentSetting.timeOut;


            try
            {
                dbConnection.Open();
            }

            catch (Exception)
            {
                message = "Fail to connect the SQL server";
                isExecute = false;               
            }

            if (isExecute == true)
            {
                SqlDataAdapter da = new SqlDataAdapter(sql);

                try
                {
                    da.Fill(currentOutput);                    
                    dbConnection.Close();
                    da.Dispose();
                    message = "Success";

                    if (currentSetting.command == "REMOVESQLROW.ANDCONDITION" || currentSetting.command == "REMOVESQLROW.ORCONDITION" || currentSetting.command == "REMOVESQLROW.ANDCONDITIONTABLE" || currentSetting.command == "REMOVESQLROW.ORCONDITIONTABLE")
                    {
                        message = "Filtered rows are removed";                       
                        isExecute = false;                       
                    }
                }

                catch (Exception)
                {
                    message = "Fail to execute the SQL Statement: " + currentSetting.sqlStatement;
                    isExecute = false;
                }
            }

            if (isExecute == true)
            {
                LedgerRAM currentProcess = new LedgerRAM();
                dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
                dataTable2LedgerRAM = currentProcess.dataTable2LedgerRAM(currentOutput, setDataTable2LedgerRAM);
            }
         
            return (dataTable2LedgerRAM, message);
        }
        public (DataTable, string) runSQL2DataTable(SQLsetting currentSetting)
        {
            string message = null;

            bool isExecute = true;

            LedgerRAM dataTable2LedgerRAM = new LedgerRAM();

            DataTable currentOutput = new DataTable();
            SqlConnection dbConnection = new SqlConnection(currentSetting.currentConnectionString);
            SqlCommand sql = new SqlCommand(currentSetting.sqlStatement, dbConnection);
            sql.CommandTimeout = currentSetting.timeOut;


            try
            {
                dbConnection.Open();
            }

            catch (Exception)
            {
                message = "Fail to connect the SQL server";
                isExecute = false;
            }

            if (isExecute == true)
            {
                SqlDataAdapter da = new SqlDataAdapter(sql);

                try
                {
                    da.Fill(currentOutput);
                    dbConnection.Close();
                    da.Dispose();
                    message = "Success";

                    if (currentSetting.command == "REMOVESQLROW.ANDCONDITION" || currentSetting.command == "REMOVESQLROW.ORCONDITION" || currentSetting.command == "REMOVESQLROW.ANDCONDITIONTABLE" || currentSetting.command == "REMOVESQLROW.ORCONDITIONTABLE")
                    {
                        message = "Filtered rows are removed";
                        isExecute = false;
                    }
                }

                catch (Exception)
                {
                    message = "Fail to execute the SQL Statement:----------------------" + currentSetting.sqlStatement;
                    isExecute = false;
                }
            }          

            return (currentOutput, message);
        }
        public (LedgerRAM, string) filterSQL2LedgerRAMByConditionTable(LedgerRAM conditionTable, SQLsetting currentSetting)
        {
            LedgerRAM currentTable = new LedgerRAM();
            string message = null;
            currentSetting.sentSQLstatement = 0;

            Dictionary<string, string> compareOperatorDict = new Dictionary<string, string>();
            compareOperatorDict.Add(">=", "Greater Than or Equal");
            compareOperatorDict.Add(">", "Greater Than");
            compareOperatorDict.Add("<=", "Less Than or Equal");
            compareOperatorDict.Add("<", "Less Than");
            compareOperatorDict.Add("!=", "Not Equal");
            compareOperatorDict.Add("=", "Equal");
            compareOperatorDict.Add("..", "Range");

            Dictionary<string, string> reserveSpecialCharDict = new Dictionary<string, string>();
            reserveSpecialCharDict.Add(">", null);
            reserveSpecialCharDict.Add("<", null);
            reserveSpecialCharDict.Add("=", null);
            reserveSpecialCharDict.Add("!", null);
            reserveSpecialCharDict.Add(".", null);
            reserveSpecialCharDict.Add(",", null);
            reserveSpecialCharDict.Add("\"", null);

            SQL newSQL = new SQL();
            SQLsetting setSQLsetting = new SQLsetting();
            setSQLsetting.currentSQLServer = currentSetting.currentSQLServer;
            setSQLsetting.currentConnectionString = currentSetting.currentConnectionString;
            setSQLsetting.sqlStatement = "Select top 10 * from " + currentSetting.sourceTable;
            setSQLsetting.resultTable = currentSetting.resultTable;            

            string classMessage;

            (currentTable, classMessage) = newSQL.SQL2LedgerRAM(setSQLsetting);           

            Dictionary<string, int> upperColumnName2ID = new Dictionary<string, int>();

            foreach (var pair in currentTable.columnName)
                upperColumnName2ID.Add(pair.Value.ToUpper(), pair.Key);

            List<int> selectedColumnID = new List<int>();

            for (int x = 0; x < conditionTable.columnName.Count; x++)
                selectedColumnID.Add(upperColumnName2ID[conditionTable.columnName[x].ToUpper()]);       
         
            LedgerRAM currentOutput = new LedgerRAM();
            DataTable DT = new DataTable();
            ConcurrentDictionary<int, DataTable> dtMultithread = new ConcurrentDictionary<int, DataTable>();           

            SQL newFilter = new SQL();
            SQLsetting setFilter = new SQLsetting();

            setFilter.currentSQLServer = currentSetting.currentSQLServer;
            setFilter.currentConnectionString = currentSetting.currentConnectionString;
            setFilter.sourceTable = currentSetting.sourceTable;
            setFilter.filterType = currentSetting.filterType;
            setFilter.command = currentSetting.command;

            int maxSQLsize = currentSetting.maxSQLstatementSize;

            double batchRowCount = (conditionTable.factTable[0].Count - 1) / maxSQLsize;
            int batchSize = Convert.ToInt32(Math.Floor(batchRowCount));            

            if (currentSetting.command == "FILTERSQLROW.ANDDISTINCTTABLE" || currentSetting.command == "FILTERSQLROW.ORDISTINCTTABLE" || currentSetting.command == "REMOVESQLROW.ANDDISTINCTTABLE" || currentSetting.command == "REMOVESQLROW.ORDISTINCTTABLE")
            {
                if (conditionTable.factTable.Count > 1 && conditionTable.factTable[0].Count > 100)
                {
                    StringBuilder indexing = new StringBuilder();

                    indexing.Append("DROP INDEX IF EXISTS idx_pname ON " + currentSetting.sourceTable + ";");
                    currentSetting.sqlStatement = indexing.ToString();
                    message = runNonQuerySQL(currentSetting);
                   
                    indexing.Clear();
               
                    indexing.Append("CREATE INDEX idx_pname ON " + currentSetting.sourceTable + " (");

                    for (int x = 0; x < conditionTable.columnName.Count; x++)
                    {
                        if (x < conditionTable.columnName.Count - 1)
                            indexing.Append(conditionTable.columnName[x].Replace(" ", "_").Replace("/", "") + ",");
                        else
                            indexing.Append(conditionTable.columnName[x].Replace(" ", "_").Replace("/", "") + ");");
                    }

                    currentSetting.sqlStatement = indexing.ToString();                  

                    message = runNonQuerySQL(currentSetting);
                }
            }

            if (currentSetting.selectedColumnName != null)
            {
                StringBuilder indexing = new StringBuilder();

                indexing.Append("DROP INDEX IF EXISTS idx_pname ON " + currentSetting.sourceTable + ";");
                currentSetting.sqlStatement = indexing.ToString();
                message = runNonQuerySQL(currentSetting);
               
                indexing.Clear();
           
                if (currentSetting.selectedColumnName.Count > 1 && conditionTable.factTable[0].Count > 100)
                {
                    indexing.Append("CREATE INDEX idx_pname ON " + currentSetting.sourceTable + " (");

                    for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
                    {
                        if (x < currentSetting.selectedColumnName.Count - 1)
                            indexing.Append(currentSetting.selectedColumnName[x].Replace(" ", "_").Replace("/", "") + ",");
                        else
                            indexing.Append(currentSetting.selectedColumnName[x].Replace(" ", "_").Replace("/", "") + ");");
                    }

                    currentSetting.sqlStatement = indexing.ToString();                   

                    message = runNonQuerySQL(currentSetting);
                }
            }

            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();          
            ConcurrentDictionary<int, SQLsetting> writeColumnThread = new ConcurrentDictionary<int, SQLsetting>();            

            for (int worker = 0; worker < batchSize + 1; worker++)
                writeColumnThread.TryAdd(worker, new SQLsetting());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };

            if (batchSize > 0)
            {
                /*
                Parallel.For(0, batchSize + 1, options, yy =>
                {
                    (dtMultithread[yy], message) = filterAndRemoveOneBatchSQL(checkThreadCompleted, batchSize, currentSetting, maxSQLsize, yy, setFilter, newFilter, currentOutput, message, selectedColumnID, conditionTable, compareOperatorDict, reserveSpecialCharDict);
                });

                do
                {
                    Thread.Sleep(2);

                } while (checkThreadCompleted.Count < batchSize + 1);

                */
                for (int yy = 0; yy <= batchSize; yy++)
                {
                    (dtMultithread[yy], message) = filterAndRemoveOneBatchSQL(checkThreadCompleted, batchSize, currentSetting, maxSQLsize, yy, setFilter, newFilter, currentOutput, message, selectedColumnID, conditionTable, compareOperatorDict, reserveSpecialCharDict);
                }
               
                for (int yy = 1; yy <= batchSize; yy++)
                    dtMultithread[0].Merge(dtMultithread[yy]);                

                DT = dtMultithread[0];               
            }

            if (batchSize == 0)
            {   
                (DT, message) = filterAndRemoveOneBatchSQL(checkThreadCompleted, batchSize, currentSetting, maxSQLsize, 0, setFilter, newFilter, currentOutput, message, selectedColumnID, conditionTable, compareOperatorDict, reserveSpecialCharDict);                  
            }

            if (message == null)
                message = "Success";

            if (!message.Contains("Fail"))
            {
                LedgerRAM currentProcess = new LedgerRAM();
                dataTable2LedgerRAMSetting setDataTable2LedgerRAM = new dataTable2LedgerRAMSetting();
                currentOutput = currentProcess.dataTable2LedgerRAM(DT, setDataTable2LedgerRAM);
            }

            return (currentOutput, message);
        }
        public (DataTable, string) filterAndRemoveOneBatchSQL(ConcurrentQueue<int> checkThreadCompleted, int batchSize, SQLsetting currentSetting, int maxSQLsize, int yy, SQLsetting setFilter, SQL newFilter, LedgerRAM currentOutput, string message, List<int> selectedColumnID, LedgerRAM conditionTable, Dictionary<string, string> compareOperatorDict, Dictionary<string, string> reserveSpecialCharDict)
        {
           
            int SQLrowCount = 0;
            StringBuilder sqlStatementGroup = new StringBuilder();
            DataTable DT = new DataTable();

            if (currentSetting.command == "FILTERSQLROW.ANDCONDITIONTABLE" || currentSetting.command == "FILTERSQLROW.ORCONDITIONTABLE" || currentSetting.command == "FILTERSQLROW.ANDDISTINCTTABLE" || currentSetting.command == "FILTERSQLROW.ORDISTINCTTABLE")
                sqlStatementGroup.Append(" Select * from " + currentSetting.sourceTable + " Where ");

            if (currentSetting.command == "REMOVESQLROW.ANDCONDITIONTABLE" || currentSetting.command == "REMOVESQLROW.ORCONDITIONTABLE" || currentSetting.command == "REMOVESQLROW.ANDDISTINCTTABLE" || currentSetting.command == "REMOVESQLROW.ORDISTINCTTABLE")
                sqlStatementGroup.Append(" Delete from " + currentSetting.sourceTable + " Where ");           

            if (batchSize > 0)
            {
                if (yy != batchSize)
                {                    
                    for (int y = 1 + (yy * maxSQLsize); y < (1 + maxSQLsize) + (yy * maxSQLsize); y++)
                    {
                        List<string> selectedColumnName = new List<string>();
                        Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
                        Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();

                        filterAndRemove(y, selectedColumnName, compareOperator, selectedTextNumber);

                        setFilter.selectedColumnName = selectedColumnName;
                        setFilter.compareOperator = compareOperator;
                        setFilter.selectedTextNumber = selectedTextNumber;                       

                        sqlStatementGroup.Append("(" + newFilter.createSQLstatement(setFilter) + ")");

                        if (y < (1 + maxSQLsize) + (yy * maxSQLsize) - 1)
                            sqlStatementGroup.Append(" Or ");

                        SQLrowCount = maxSQLsize;
                    }
                }
              
                if (yy == batchSize)
                {
                    int yScenario = maxSQLsize * batchSize + 1;
                    
                    if (yScenario != conditionTable.factTable[0].Count)
                    {
                        for (int y = yScenario; y < conditionTable.factTable[0].Count; y++)
                        {
                            List<string> selectedColumnName = new List<string>();
                            Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
                            Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();

                            filterAndRemove(y, selectedColumnName, compareOperator, selectedTextNumber);

                            setFilter.selectedColumnName = selectedColumnName;
                            setFilter.compareOperator = compareOperator;
                            setFilter.selectedTextNumber = selectedTextNumber;

                            sqlStatementGroup.Append("(" + newFilter.createSQLstatement(setFilter) + ")");

                            if (y < conditionTable.factTable[0].Count - 1)
                                sqlStatementGroup.Append(" Or ");

                            SQLrowCount = conditionTable.factTable[0].Count - yScenario;
                        }
                    }
                }
            }

            if(batchSize == 0)
            {
             
                for (int y = 1; y < conditionTable.factTable[0].Count; y++)
                {
                    List<string> selectedColumnName = new List<string>();
                    Dictionary<int, List<string>> compareOperator = new Dictionary<int, List<string>>();
                    Dictionary<int, List<string>> selectedTextNumber = new Dictionary<int, List<string>>();

                    filterAndRemove(y, selectedColumnName, compareOperator, selectedTextNumber);

                    setFilter.selectedColumnName = selectedColumnName;
                    setFilter.compareOperator = compareOperator;
                    setFilter.selectedTextNumber = selectedTextNumber;                                

                    sqlStatementGroup.Append("(" + newFilter.createSQLstatement(setFilter) + ")");

                    if (y < conditionTable.factTable[0].Count - 1)
                        sqlStatementGroup.Append(" Or ");

                    SQLrowCount = conditionTable.factTable[0].Count - 1;
                }
            }

            sqlStatementGroup.Append(";");

            currentSetting.sqlStatement = sqlStatementGroup.ToString();

            currentSetting.sentSQLstatement = currentSetting.sentSQLstatement + SQLrowCount;


            Console.Write("Sent SQL Statements:" + currentSetting.sentSQLstatement + " ");          

            (DT, message) = runSQL2DataTable(currentSetting);

           // Console.WriteLine(yy + "  " + message);

            checkThreadCompleted.Enqueue(yy);

            return (DT, message);          

            //message = runNonQuerySQL(currentSetting);           

            void filterAndRemove(int y, List<string> selectedColumnName, Dictionary<int, List<string>> compareOperator, Dictionary<int, List<string>> selectedTextNumber)
            {
                bool isOperatorExist;
                int startAddress;

                Dictionary<int, bool> isRangeExist = new Dictionary<int, bool>();
                StringBuilder currentCell = new StringBuilder();

                for (int x = 0; x < selectedColumnID.Count; x++)
                {
                    isOperatorExist = false;

                    selectedColumnName.Add(conditionTable.columnName[x].Trim());                  

                    currentCell.Append(conditionTable.key2Value[x][conditionTable.factTable[x][y]]);

                    startAddress = 0;

                    if (!compareOperator.ContainsKey(x))
                        compareOperator.Add(x, new List<string>());

                    if (!selectedTextNumber.ContainsKey(x))
                        selectedTextNumber.Add(x, new List<string>());

                    for (int i = 0; i < currentCell.Length; i++)
                    {
                        if (currentCell.Length >= 4 && i < currentCell.Length - 1)
                        {
                            if (compareOperatorDict.ContainsKey(currentCell.ToString().Substring(i, 2)))
                            {
                                if (currentCell.ToString().Substring(i, 2) == "..")
                                {
                                    compareOperator[x].Add(">=");
                                    compareOperator[x].Add("<=");
                                    isOperatorExist = true;

                                    if (!isRangeExist.ContainsKey(x))
                                        isRangeExist.Add(x, true);

                                    if (!selectedTextNumber.ContainsKey(x))
                                        selectedTextNumber.Add(x, new List<string>());

                                    selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, i - startAddress).Trim());
                                }
                                else
                                {
                                    compareOperator[x].Add(currentCell.ToString().Substring(startAddress, 2).Trim());
                                    isOperatorExist = true;
                                }

                                i++;
                                startAddress = i;
                            }
                        }

                        if (compareOperatorDict.ContainsKey(currentCell.ToString().Substring(i, 1)))
                        {
                            compareOperator[x].Add(currentCell.ToString().Substring(i, 1).Trim());
                            isOperatorExist = true;

                            if (!isRangeExist.ContainsKey(x))
                                isRangeExist.Add(x, false);

                            startAddress = i;
                        }

                        if (isOperatorExist == false && i == currentCell.Length - 1)
                            compareOperator[x].Add("=");

                        if (currentCell.ToString().Substring(i, 1) == ",")
                        {
                            if (isOperatorExist == false)
                                compareOperator[x].Add("=");

                            if (reserveSpecialCharDict.ContainsKey(currentCell.ToString().Substring(startAddress, 1)))
                            {
                                selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress + 1, i - startAddress - 1).Trim());
                            }
                            else
                            {
                                selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, i - startAddress).Trim());
                            }

                            startAddress = i;
                        }
                        else if (i == currentCell.Length - 1)
                        {
                            if (reserveSpecialCharDict.ContainsKey(currentCell.ToString().Substring(startAddress, 1)))
                            {
                                selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress + 1, currentCell.Length - startAddress - 1).Trim());
                            }
                            else
                            {
                                selectedTextNumber[x].Add(currentCell.ToString().Substring(startAddress, currentCell.Length - startAddress).Trim());
                            }

                            startAddress = i;
                        }
                    }

                    currentCell.Clear();
                }
            }
        }

        public (LedgerRAM, string) groupSQLTableBy(SQLsetting currentSetting)
        {
            /*

            for (int x = 0; x < currentSetting.groupByColumnName.Count; x++)
                Console.WriteLine("groupByColumnName " + currentSetting.groupByColumnName[x]);

            for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
                Console.WriteLine("selectedColumnName " + currentSetting.selectedColumnName[x]);

            for (int x = 0; x < currentSetting.aggregateFunction.Count; x++)
                Console.WriteLine("aggregateFunction " + currentSetting.aggregateFunction[x]);

            for (int x = 0; x < currentSetting.aggregateByColumnName.Count; x++)
                Console.WriteLine("aggregateByColumnName " + currentSetting.aggregateByColumnName[x]);

            */

            StringBuilder sqlCommand = new StringBuilder();

            sqlCommand.Append("Select ");

            for (int x = 0; x < currentSetting.aggregateByColumnName.Count; x++)
            {
                if (currentSetting.aggregateFunction[x].ToUpper() != "COUNT")
                    sqlCommand.Append(currentSetting.aggregateFunction[x] + "(" + currentSetting.aggregateByColumnName[x].Replace(" ", "_").Replace("/", "") + ") ");

                if (currentSetting.aggregateFunction[x].ToUpper() == "COUNT")
                    sqlCommand.Append(currentSetting.aggregateFunction[x] + "(" + currentSetting.selectedColumnName[0].Replace(" ", "_").Replace("/", "") + ") ");

                if (currentSetting.aggregateFunction[x].ToUpper() == "SUM")
                    sqlCommand.Append("as " + currentSetting.aggregateByColumnName[x].Replace(" ", "_").Replace("/", ""));

                if (currentSetting.aggregateFunction[x].ToUpper() == "MAX")
                    sqlCommand.Append("as " + "Max$" + currentSetting.aggregateByColumnName[x].Replace(" ", "_").Replace("/", ""));

                if (currentSetting.aggregateFunction[x].ToUpper() == "MIN")
                    sqlCommand.Append("as " + "Min$" + currentSetting.aggregateByColumnName[x].Replace(" ", "_").Replace("/", ""));

                if (currentSetting.aggregateFunction[x].ToUpper() == "COUNT")
                    sqlCommand.Append("as " + "Count");

                sqlCommand.Append(", ");
            }

            for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
            {
                sqlCommand.Append(currentSetting.selectedColumnName[x].Replace(" ", "_").Replace("/", ""));

                if (x < currentSetting.selectedColumnName.Count - 1)
                    sqlCommand.Append(",");
            }

            sqlCommand.Append(" from " + currentSetting.sourceTable + " Group By ");

            for (int x = 0; x < currentSetting.selectedColumnName.Count; x++)
            {
                sqlCommand.Append(currentSetting.selectedColumnName[x].Replace(" ", "_").Replace("/", ""));

                if (x < currentSetting.selectedColumnName.Count - 1)
                    sqlCommand.Append(",");
            }

            currentSetting.sqlStatement = sqlCommand.ToString();
            currentSetting.command = currentSetting.command;            

            LedgerRAM currentOutput = new LedgerRAM();
            string classMessage = null;
            SQL newSQL = new SQL();

            (currentOutput, classMessage) = newSQL.SQL2LedgerRAM(currentSetting);           

            return (currentOutput, classMessage);
        }
    }
}
