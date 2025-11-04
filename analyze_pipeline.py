from analyze_step1a_combined import main_combined
from unused.contracts_amount import count_rows

#Runs all analysis

#AllThem = False

Governor = [
"Fortuño",
"Padilla", 
"Rosello", 
"Pierluisi(De_Facto)", 
"Vazquez", 
"Pierluisi", 
"Gonzalez"
]


DATE_FROM = ""
DATE_TO = ""
def gov(Governor):
    if Governor == "Fortuño":
        DATE_FROM = "2009-01-02"
        DATE_TO = "2013-01-01" # One day before the other takes office
    if Governor == "Padilla":
        DATE_FROM = "2013-01-02"
        DATE_TO = "2017-01-01"
    elif Governor == "Rosello":
        DATE_FROM = "2017-01-02"
        DATE_TO = "2019-08-01"
    elif Governor == "Pierluisi(De_Facto)":
        DATE_FROM = "2019-08-02"
        DATE_TO = "2019-08-06"
    elif Governor == "Vazquez":
        DATE_FROM = "2019-08-07"
        DATE_TO = "2021-01-01"
    elif Governor == "Pierluisi":
        DATE_FROM = "2021-01-02"
        DATE_TO = "2025-01-01"
    elif Governor == "Gonzalez":
        DATE_FROM = "2025-01-02"
        DATE_TO = "2029-01-01"
    return DATE_FROM, DATE_TO

#for g in Governor:
#    DATE_FROM, DATE_TO = gov(g)
#    how = count_rows(DATE_FROM,DATE_TO)
#    print(f"\n{how} Rows for {g}")



Metrics = ["Contractors", "Service", "EntityName"]


for g in Governor:
    for m in Metrics:
        DATE_FROM, DATE_TO = gov(g)
        print(DATE_FROM, DATE_TO)
        print(m)
        print(g)

        main_combined(DATE_FROM, DATE_TO, g,m)