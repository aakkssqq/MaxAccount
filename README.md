# MaxAccount

MaxAccount is not a fixed accounting software, however it looks like an Accounting ETL supports your customisation of accounting rules to adapt every change of statutory and management requirement.

It is a .net console project and has some of dependencies:

- .net framework (it supports .net 4.7.2, .net 4.8, .netcore 3.1)
- .net System.Data.SqlClient https://www.nuget.org/packages/System.Data.SqlClient/
- MaxAccountExtension https://www.nuget.org/packages/MaxAccountExtension/1.0.0

MaxAccountExtension is a commercial library, you can download it for trial basis. If you opt to avoid using this library, amendments to certain source codes of the open source project are necessary.

After you download the above repository, you can open and build by Visual Studio 2019 Community directly after making a reference to "MaxAccountExtension". 
Otherwise, you may copy the source code folder "Controller", "Conversion", "Model" and "Program.cs" to your Visual Studio project folder for further actions.

How-to videos will be published on the YouTube channel "MaxAccount" 
https://www.youtube.com/channel/UCouJHDI_7dkNbiEnuDpnFmg

Video 1 How to build and run: https://youtu.be/oEjBtHElH7w

Video 2 Command List and Syntax: https://youtu.be/UQbJB_vaK5Y

Video 3 VoucherEntry: https://youtu.be/wkDV8hWaIyI

- CSV2LedgerRAM{Invoice.csv ~ trading}
- VoucherEntry{Credit(Sales) Debit(Account Receivable) => Amount}
- SelectColumn{Date, D/C, Account, Invoice No,Customer, Product, Amount}
-  OrderBy{Date(A) Invoice No(A) D/C(D)}
- LedgerRAM2CSV{trading | * ~ Result-VoucherEntry.csv}

Video 4 Distinct: https://youtube.com/shorts/di4gc2Thi44?feature=share

- CSV2LedgerRAM{Transaction.csv ~ Table}
- Distinct{Customer,Product ~ Distinct}
- LedgerRAM2CSV{Distinct  | * ~ Result-DisctictByProductAndCustomer.csv}

Video 5 GroupBy: https://youtube.com/shorts/8-WMIX3LwjA?feature=share

- CSV2LedgerRAM{Transaction.csv ~ Table}   
- GroupBy{Customer, Product,Unit Code,Currency => Sum(Quantity) Sum(Amount) ~ GroupByCustomerAndProduct}
- LedgerRAM2CSV{GroupByCustomerAndProduct | * ~ Result-GroupByCustomerAndProduct.csv}

Video 6 Crosstab : https://youtube.com/shorts/me7ZfQSFn0Y?feature=share
- CSV2LedgerRAM{Transaction.csv ~ Table}  
- Crosstab{X(Customer, Currency) Y(Product,Unit Code) => Count() Sum(Quantity) Sum(Base Amount)}    
- LedgerRAM2CSV{Table | * ~ Result-CrazyCrosstab.csv}

Video 7 ReverseCrosstab : Coming soon

Video 8 AndFilter: https://youtube.com/shorts/Lp2_aBH_mqU?feature=share
- CSV2LedgerRAM{Transaction.csv ~ Table}    
- AndFilter{Table | Product(=Apple,=Orange) Amount(500..1000) ~ Table1}   
- LedgerRAM2CSV{Table1 | * ~ Result-AndFilterByCustomerAndProductApple.csv}

Video 9 JoinTable : Coming soon

Video 10 ComputeColumn : Coming soon

Video 11 and after are planning in progress.

Relevent data and rule files will be uploaded to the folder "UseCase" of this repository.
