# FEAST

## Feast is python API to build [Feature Store](https://feast.dev/)


### **LOCAL RUN**

    * run 'bash .github/workflows/local.sh'
        - It brings up Redis instance on your local machine. Make sure that docker is installed locally.
        - It run 3 steps inside the script
            1. APPLY : this published the changes of your code to FEAST
            2. MATERIALIZAE: This pull data from offline store and push data to Online store between 2 dates
            3. MATERIALIZAE-INCREMENTAL: This pull data from offline store and push data to Online store from last fetched data to end date provided

### **PRODUCTION/STAGING Deployment**






