# MaxAccount

MaxAccount is not a fixed accounting software, however it supports your customisation of accounting rules to support every change of statutory and management requirements.

It is a .net console project and has some of dependencies:

1) .net framework (it supports .net 4.7.2, .net 4.8, .netcore 3.1)
2) .net System.Data.SqlClient https://www.nuget.org/packages/System.Data.SqlClient/
3) MaxAccountExtension https://www.nuget.org/packages/MaxAccountExtension/1.0.0

MaxAccountExtension is a commercial library, you can download it for trial basis. If you opt to avoid using this library, amendments to certain source codes of the open source project are necessary.

After you download the above repository, you can open and build by Visual Studio 2019 Community directly after making a reference to "MaxAccountExtension". 
Otherwise, you may copy the source code folder "Controller", "Conversion", "Model" and "Program.cs" to your Visual Studio project folder for further actions.

How-to videos will be published on the YouTube channel "Lami Yu" https://www.youtube.com/channel/UCouJHDI_7dkNbiEnuDpnFmg

Video 1 How to build and run: https://youtu.be/oEjBtHElH7w

Video 2 VoucherEntry: https://youtu.be/wkDV8hWaIyI

Video 3 Distinct: https://youtu.be/di4gc2Thi44

Video 4 GroupBy: Coming soon

Video 5 Compare different between Distinct and GroupBy : Coming soon

Video 6 Crosstab : Coming soon

Video 7 Compare different between Crosstab and GroupBy : Coming soon

Video 8 JoinTable : Coming soon

Video 9 ComputeColumn : Coming soon

Video 10 Compare different between JoinTable and ComputeColumn : Coming soon

Record video 11 and after are planning in progress.

Relevent data and rule files will be uploaded to the folder "UseCase" of this repository.
