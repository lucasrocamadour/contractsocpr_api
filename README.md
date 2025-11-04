# contractsocpr_api
An api to scrape the Consulta del Registro de Contratos website

## Description
First Step: Downloads the data displayed as rows in the websites and normalizes it. It also analyzes any outliers or anomalies so we can add them to the json to be fixed or dropped once we look at the official contract.

Second Step: Select by candidates by their respective dates and aggregate contracts by 3 main groups: Service, Contractors and Entity