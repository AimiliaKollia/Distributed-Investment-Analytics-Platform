Make sure Kafka, MySQL, and MongoDB are running before executing the applications.
helpers/ contains shared logic used across the project (e.g., configuration, date handling and investor engine utilities)


Run all commands from the root of the project:

python3 src/investors/investorsDB.py
python3 src/se1_server.py
python3 src/se2_server.py
python3 src/investors/inv1.py
python3 src/investors/inv2.py
python3 src/investors/inv3.py
python3 src/apps/app1.py
python3 src/apps/app2.py <InvestorName>
python3 src/apps/app3.py
python3 src/apps/app4.py <PortfolioName> <StartDate> <EndDate>

Parameters:
App2
<InvestorName> = one of:

Inv1
Inv2
Inv3

Example
python3 src/apps/app2.py Inv1

App4
<PortfolioName> = one of: P11, P12, P21, P22, P31, P32
<StartDate> and <EndDate> format: YYYY-MM-DD

Example
python3 src/apps/app4.py P11 2020-09-19 2020-09-21


