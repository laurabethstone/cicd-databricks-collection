# cicd-databricks-helpers
[all scopes of databricks project CICD]

- 3p batch processer
  * ActiveBatch
    - runs the deploy or other env scrips
    - ssh github repo to AB
  * Delta SymlinkManafest
  * aws Role permissions -> table owner

    
- spark w/ databricks dbt
  * run local context
  * run ui all purpose
  * run job w/ deploy local
  * run job w/ github actions pipenv [script] "deploy.py"
  * common utils IO wrapper
  * - logger
    - set context
    - dbutils widgets
  * runtime
  * self python functions _get(spark)

    
- mlops
  * infastructure as code (iac)
  * - terraform
  * - jinja2
      * json.j2 or yaml deploy file
      * use native jinja env variable w/ venv setup
      * use jinja variable w/ dbutils and widgets
      * use jinja template to auto populate
  * build the deploy api componets dynamically
  * github actions
  * github runners
  * permission chain aws -> appId

  
- pytest
  * Unit
  * Ingest
  * Checks
    


- databricks-api
- 

- tableau-api-lib
  * control the analytic end point
  
- resources
  * ec2 selection
  * cluster monitoring
  * setting spark conf



- bi analytic golden rules
  * users will want a data dictionary
  * users will want visual consistency
  * users will want simple not loud
  * users will want flexibility of choice
  * users will want to export data by their choosing
  * users will want the data to match their CMS
    -- use sys report as viz source
 

- tbd
