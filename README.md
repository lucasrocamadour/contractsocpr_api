# contractsocpr_api
An api to scrape the Consulta del Registro de Contratos website.

## Description
First Step: Downloads the data displayed in the search tables. Then it normalizes it. Optionally, it analyzes any outliers or anomalies. These anomalies can then be added to the fix.json to be fixed or dropped once we look at the official contract.

Second Step: Filter candidates by their respective dates, then aggregate contracts by 3 main groups: Service, Contractors and Entity.