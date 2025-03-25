# **Data Modeling for Data Engineers**

## **1. Introduction to Data Modeling**
Data modeling is the process of structuring and organizing data for efficient storage, retrieval, and management. It plays a crucial role in database design, data warehousing, and big data processing.

## **2. Types of Data Models**
Data modeling is categorized into three main types:
- **Conceptual Data Model**: High-level design, focusing on entities and relationships.
- **Logical Data Model**: Detailed structure including attributes, keys, and relationships.
- **Physical Data Model**: Implementation-specific schema design including indexing, partitioning, and storage details.

## **3. Star Schema**
Star Schema is a denormalized design used in data warehousing where a central fact table is connected to dimension tables.

### **3.1 Structure of Star Schema**
- **Fact Table**: Stores business events (e.g., sales transactions).
- **Dimension Tables**: Stores descriptive attributes (e.g., customers, products, time).

### **3.2 Example Schema**
```sql
CREATE TABLE FactSales (
    SaleID INT PRIMARY KEY,
    DateID INT,
    ProductID INT,
    CustomerID INT,
    Amount DECIMAL(10,2),
    FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID)
);

CREATE TABLE DimDate (
    DateID INT PRIMARY KEY,
    Year INT,
    Month INT,
    Day INT
);

CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    Category VARCHAR(100)
);

CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Location VARCHAR(100)
);
```

## **4. Snowflake Schema**
Snowflake Schema is a normalized design that reduces data redundancy by further breaking down dimension tables.

### **4.1 Structure of Snowflake Schema**
- **Fact Table**: Same as in Star Schema.
- **Dimension Tables**: Normalized into multiple related tables.

### **4.2 Example Schema**
```sql
CREATE TABLE DimProductCategory (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(100)
);

CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    CategoryID INT,
    FOREIGN KEY (CategoryID) REFERENCES DimProductCategory(CategoryID)
);
```

## **5. Fact and Dimension Modeling**
### **5.1 Fact Table**
- Stores measurable business events.
- Contains foreign keys referencing dimension tables.
- Example:
```sql
CREATE TABLE FactSales (
    SaleID INT PRIMARY KEY,
    DateID INT,
    ProductID INT,
    CustomerID INT,
    Amount DECIMAL(10,2)
);
```

### **5.2 Dimension Table**
- Stores descriptive attributes.
- Example:
```sql
CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Location VARCHAR(100)
);
```

## **6. NoSQL Data Modeling**
NoSQL databases are designed for scalability and flexibility. Common models include:

### **6.1 Wide-Column Store (Cassandra Example)**
```cql
CREATE TABLE orders_by_customer (
    customer_id UUID,
    order_id UUID,
    order_date TIMESTAMP,
    total_amount DECIMAL,
    PRIMARY KEY (customer_id, order_id)
);
```

### **6.2 Document Store (MongoDB Example)**
```json
{
    "customer_id": "C123",
    "name": "John Doe",
    "orders": [
        {"order_id": "O1001", "total_amount": 250},
        {"order_id": "O1002", "total_amount": 180}
    ]
}
```

## **7. Slowly Changing Dimensions (SCDs)**
SCDs track historical changes in dimension tables.

### **7.1 SCD Type 1 (Overwrite Old Data)**
```sql
UPDATE DimCustomer SET Email = 'new@example.com' WHERE CustomerID = 101;
```

### **7.2 SCD Type 2 (Maintain History with Versioning)**
```sql
ALTER TABLE DimCustomer ADD COLUMN Version INT;
INSERT INTO DimCustomer (CustomerID, CustomerName, Email, Version) VALUES (101, 'John Doe', 'new@example.com', 2);
```

### **7.3 SCD Type 3 (Maintain Old and New Values)**
```sql
ALTER TABLE DimCustomer ADD COLUMN PreviousEmail VARCHAR(100);
UPDATE DimCustomer SET PreviousEmail = Email, Email = 'new@example.com' WHERE CustomerID = 101;
```

## **8. Conclusion**
Data modeling is essential for designing scalable and efficient databases. Understanding different modeling techniques helps data engineers build optimized solutions for transactional and analytical workloads.

## **SCD Type 2 (Maintain Full History with Versioning or Date Ranges)**
How it works:
A new row is inserted every time an attribute changes.
The old record remains in the table with an EndDate (or an active/inactive flag).
Allows tracking of all historical changes.
Use case: If we need a complete history of all changes over time.
```sql
Example Table: Employee_SCD2

EmployeeID	Name	Department	Salary	StartDate	EndDate	ActiveFlag
101	John Doe	HR	50000	2022-01-01	2023-03-31	N
101	John Doe	IT	55000	2023-04-01	NULL	Y
Implementation (SCD Type 2 - New Row Inserted)

-- Step 1: Mark the old record as inactive (if exists)
UPDATE Employee_SCD2 
SET EndDate = '2023-03-31', ActiveFlag = 'N'
WHERE EmployeeID = 101 AND ActiveFlag = 'Y';

-- Step 2: Insert a new row with updated information
INSERT INTO Employee_SCD2 (EmployeeID, Name, Department, Salary, StartDate, EndDate, ActiveFlag)
VALUES (101, 'John Doe', 'IT', 55000, '2023-04-01', NULL, 'Y');
```
Advantages:
Maintains full history of changes.
Easy to query for past data.
Disadvantages:
Table grows larger over time.

## **SCD Type 3 (Limited History with Previous Value Storage)**

How it works:
Instead of inserting a new row, a new column is added to store the previous value.
Retains only one level of historical change.
Use case: If we only care about the current and previous value, not the full change history.
```sql
Example Table: Employee_SCD3
EmployeeID	Name	Current_Department	Previous_Department	Current_Salary	Previous_Salary
101	John Doe	IT	HR	55000	50000
Implementation (SCD Type 3 - Overwrite with Previous Values)

UPDATE Employee_SCD3
SET Previous_Department = Current_Department,
    Current_Department = 'IT',
    Previous_Salary = Current_Salary,
    Current_Salary = 55000
WHERE EmployeeID = 101;
```
Advantages:
Saves storage space by not adding new rows.
Simple to query.
Disadvantages:
Only stores one level of history, so older changes are lost.
Key Differences Between SCD Type 2 and SCD Type 3
Feature	SCD Type 2	SCD Type 3
New Row Inserted?	✅ Yes	❌ No
Stores Full History?	✅ Yes (All Changes)	❌ No (Only Last Change)
Storage Growth?	🔺 Increases Over Time	🔻 Constant Size
Easy to Track Historical Data?	✅ Yes	❌ No
When to Use Each?
Use SCD Type 2 when full historical tracking is needed (e.g., salary, department changes over multiple years).
Use SCD Type 3 when only recent changes matter (e.g., tracking a temporary job promotion).

