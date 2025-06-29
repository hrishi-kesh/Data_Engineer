# 🔄 Incremental Data Load in Azure Data Factory

## 📺 Learning Resource
- **Tutorial**: 1. "Azure Data Engineer Project | Incremental Data Load in Azure Data Factory"
- **Focus**: Implementation of incremental data load patterns in ADF

## 🎯 Process Overview
Learn how to efficiently transfer new/updated data using Azure Data Factory pipelines.

## 🛠️ Implementation Steps

### 1. Initial Setup
- 🔗 **Configure Linked Services**
    - Connect ADF to source SQL Database
    - Connect ADF to target SQL Database

- 📊 **Define Datasets**
    - Source: `orders` table
    - Target: `order_final` table

### 2. Pipeline Development
- 🏗️ **Create Basic Pipeline Structure**

- 🔍 **Configure Lookup Activity** (`Lookup1`)
    ```sql
    SELECT MAX(inserttime) AS date1 FROM order_final
    ```
    > Retrieves latest watermark timestamp

- 📝 **Setup Copy Data Activity**
    ```sql
    SELECT order_id, name, inserttime 
    FROM orders 
    WHERE inserttime > '@{activity('Lookup1').output.firstRow.date1}'
    ```
    > Copies only new/modified records

### 3. Production Deployment
- ⏰ **Set Up Triggers**: Schedule automated runs
- 📊 **Monitor Pipeline**: Track execution & handle errors

## 🎉 Success Criteria
- ✅ Only new/modified data transferred
- ✅ Efficient resource utilization
- ✅ Automated execution
