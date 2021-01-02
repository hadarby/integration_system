# CRM/Dialer Integration System

### Implementing a generic CRM/dialer integration system to OptimlaQ
##### Industry Project - OptimalQ 

##### Description: 
This project is an integration system between OptimalQ and any CRM/dialer system.
<br/>
The system is written in a generic way so whenever a new integration is needed, there would be minimal code required.
<br/>
The project includes an integration to Genesys dialer as an example   
###
##### Keywords: 
python, kafka, project, CRM, dialer, setup, development, optimalq, genesys.
###

##### By Hadar Ben-Yaakov
[LinkedIn](https://www.linkedin.com/in/hadar-ben-yaakov/)
##
 
### Setup and run instructions:

#### Prerequisites:
`pip install -r requiremenets.txt`
<br/>

#### Configurations:
1. Setup kafka url, username and password
2. Setup username, password, pool_uid and url for OptimalQ
3. Setup client_id, client_secret and queue_id for Genesys 
<br/>
 
#### Run:
Run all the files in run_files with your IDE's configuration, or from the terminal.
 
 ##
 
### Usage:
To implement a new integration, implement the relevant abstract functions and create run files with the relevant setup.