--Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'ffsvf gdvfvfbg bdf';

--Create database scoped credential
CREATE DATABASE SCOPED CREDENTIAL SoraIdentity
WITH IDENTITY='Managed Identity'

--Create data source
CREATE EXTERNAL DATA SOURCE [SoraData]
WITH (
    LOCATION = 'abfss://filesystem@flumenbaron.dfs.core.windows.net/synapse/workspaces/warehouse/sora_union/',
    CREDENTIAL = [SoraIdentity]
);

--Create external file format
CREATE EXTERNAL FILE FORMAT [SoraDataCSV]
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);

--Create sora schema
CREATE SCHEMA sora

--Create Staging click table
CREATE EXTERNAL TABLE [sora].[StgClick] (
    Client VARCHAR(32),
    Project VARCHAR(85),
    Name VARCHAR(60),
    Task VARCHAR(30),
    TaskDate DATE,
    Hours FLOAT,
    Note VARCHAR(150),
    Billable NVARCHAR(9)
)
WITH (
    LOCATION = 'data/clickup.csv',
    DATA_SOURCE = [SoraData],
    FILE_FORMAT = [SoraDataCSV]
);

-- Create staging float table
CREATE EXTERNAL TABLE [sora].[StgFloat] (
    Client VARCHAR(25),
    Project VARCHAR(90),
    Role VARCHAR(90),
    Name VARCHAR(60),
    Task VARCHAR(36),
    StartDate DATE,
    EndDate DATE,
    EstimatedHours INT
)
WITH (
    LOCATION = 'data/Float-allocations.csv',
    DATA_SOURCE = [SoraData],
    FILE_FORMAT = [SoraDataCSV]
);

----Dim Client Table
CREATE TABLE [sora].[DimClient] (
    ClientID INT NOT NULL,
    ClientName VARCHAR(100) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE);

WITH ClientCTE AS (
    SELECT DISTINCT
       Client,
       ROW_NUMBER() OVER (ORDER BY Client) AS DimClientID
    FROM [sora].[StgFloat]
    WHERE Client NOT IN (SELECT ClientName FROM [sora].[DimClient])
    GROUP BY Client
)

INSERT INTO [sora].[DimClient] (ClientID, ClientName)
SELECT 
    DimClientID,
    Client
FROM ClientCTE;

-----DimTask
CREATE TABLE [sora].[DimTask] (
    TaskID INT NOT NULL,
    TaskName VARCHAR(30) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE);

-- Combine tasks from both StgClick and StgFloat
WITH CombinedTasks AS (
    SELECT DISTINCT Task
    FROM [sora].[StgClick]
    UNION
    SELECT DISTINCT Task
    FROM [sora].[StgFloat]
),

-- Select tasks that are not already in DimTask
NewTasks AS (
    SELECT Task,
           ROW_NUMBER() OVER (ORDER BY Task) AS RowNum
    FROM CombinedTasks
    WHERE Task NOT IN (SELECT TaskName FROM [sora].[DimTask])
)

-- Insert new tasks into DimTask
INSERT INTO [sora].[DimTask] (TaskID, TaskName)
SELECT 
    RowNum,
    Task
FROM NewTasks;

----DimEmployee
CREATE TABLE sora.DimEmployee (
    EmployeeID INT NOT NULL,
    EmployeeName VARCHAR(100) NOT NULL,
    Role VARCHAR(100) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE);


WITH CombinedEmployees AS (
    SELECT DISTINCT 
        Name AS EmployeeName,
        Role
    FROM [sora].[StgFloat]
    
    UNION
    
    SELECT DISTINCT 
        Name AS EmployeeName,
        NULL AS Role
    FROM [sora].[StgClick]
    WHERE Name NOT IN (SELECT Name FROM [sora].[StgFloat])
),

-- Assign Row Numbers for EmployeeID
EmployeeCTE AS (
    SELECT 
        EmployeeName,
        Role,
        ROW_NUMBER() OVER (ORDER BY EmployeeName) AS EmployeeID
    FROM CombinedEmployees
)

-- Insert into DimEmployee
INSERT INTO [sora].[DimEmployee] (EmployeeID, EmployeeName, Role)
SELECT 
    EmployeeID,
    EmployeeName,
    Role
FROM EmployeeCTE;

----DimProject
CREATE TABLE [sora].[DimProject] (
    ProjectID INT NOT NULL,
    ProjectName NVARCHAR(90) NOT NULL,
    ClientID INT NOT NULL
)
WITH (DISTRIBUTION = REPLICATE);


WITH ProjectCTE AS (
    SELECT DISTINCT
        C.ClientID,
        F.Project,
        ROW_NUMBER() OVER (ORDER BY F.Project) AS ProjectID
    FROM sora.StgFloat F
    INNER JOIN sora.DimClient C ON F.Client = C.ClientName
    WHERE F.Project NOT IN (SELECT ProjectName FROM sora.DimProject)
    GROUP BY C.ClientID, F.Project
)

INSERT INTO sora.DimProject (ProjectID, ProjectName, ClientID)
SELECT 
    ProjectID,
    Project,
    ClientID
FROM ProjectCTE;


---DimDate
CREATE TABLE [sora].[DimDate] (
    DateID INT NOT NULL,
    [TaskDate] DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    Weekday NVARCHAR(10) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE);


WITH DateCTE AS (
    SELECT DISTINCT 
        [TaskDate],
        YEAR([TaskDate]) AS Year,
        DATEPART(QUARTER, [TaskDate]) AS Quarter,
        MONTH([TaskDate]) AS Month,
        DAY([TaskDate]) AS Day,
        DATENAME(WEEKDAY, [TaskDate]) AS Weekday,
        ROW_NUMBER() OVER (ORDER BY [TaskDate]) AS RowNum
    FROM [sora].[StgClick]
    WHERE [TaskDate] NOT IN (SELECT [TaskDate] FROM [sora].[DimDate])
    GROUP BY TaskDate
)
INSERT INTO [sora].[DimDate] (DateID, [TaskDate], Year, Quarter, Month, Day, Weekday)
SELECT 
    RowNum,
    [TaskDate],
    Year,
    Quarter,
    Month,
    Day,
    Weekday
FROM DateCTE;

-------Clicks with IDs for the joins
CREATE TABLE [sora].[StgClickWithIDs] (
    Client NVARCHAR(100),
    Project NVARCHAR(100),
    Name NVARCHAR(100),
    Task NVARCHAR(100),
    Date DATE,
    Hours FLOAT,
    Note VARCHAR(150),
    Billable NVARCHAR(3),
    ClientID INT,
    ProjectID INT,
    EmployeeID INT,
    TaskID INT,
    DateID INT
)
WITH (DISTRIBUTION = REPLICATE);

INSERT INTO [sora].[StgClickWithIDs] (
    Client, Project, Name, Task, Date, Hours, Note, Billable, ClientID, ProjectID, EmployeeID, TaskID, DateID
)
SELECT 
    c.Client,
    c.Project,
    c.Name,
    c.Task,
    c.TaskDate,
    c.Hours,
    c.Note,
    c.Billable,
    dc.ClientID,
    dp.ProjectID,
    de.EmployeeID,
    dt.TaskID,
    dd.DateID
FROM [sora].[StgClick] c
LEFT JOIN [sora].[DimClient] dc ON dc.ClientName = c.Client
LEFT JOIN [sora].[DimProject] dp ON dp.ProjectName = c.Project
LEFT JOIN [sora].[DimEmployee] de ON de.EmployeeName = c.Name
LEFT JOIN [sora].[DimTask] dt ON dt.TaskName = c.Task
LEFT JOIN [sora].[DimDate] dd ON dd.TaskDate = c.TaskDate;

--Fact table
CREATE TABLE [sora].[FactTimeSheet](
    TimeEntryID INT NOT NULL,
    ClientID INT NOT NULL,
    ProjectID INT NOT NULL,
    EmployeeID INT NOT NULL,
    TaskID INT NOT NULL,
    DateID INT NOT NULL,
    Hours FLOAT NOT NULL,
    EstimatedHours FLOAT,
    Note VARCHAR(150),
    Billable BIT NOT NULL,
    StartDate DATE,
    EndDate DATE
)
WITH (DISTRIBUTION = HASH(ClientID), CLUSTERED COLUMNSTORE INDEX);

-- Insert into FactTimeSheet
INSERT INTO [sora].[FactTimeSheet] (
    TimeEntryID, ClientID, ProjectID, EmployeeID, TaskID, DateID, Hours, EstimatedHours, Note, Billable, StartDate, EndDate
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS TimeEntryID,
    sc.ClientID,
    sc.ProjectID,
    sc.EmployeeID,
    sc.TaskID,
    sc.DateID,
    sc.Hours,
    f.EstimatedHours,
    sc.Note,
    CASE WHEN sc.Billable = 'Yes' THEN 1 ELSE 0 END AS Billable,
    f.StartDate,
    f.EndDate
FROM sora.StgClickWithIDs sc
LEFT JOIN [sora].[StgFloat] f ON sc.Client = f.Client AND sc.Project = f.Project AND sc.Name = f.Name AND sc.Task = f.Task
WHERE sc.ClientID IS NOT NULL
  AND sc.ProjectID IS NOT NULL
  AND sc.EmployeeID IS NOT NULL
  AND sc.TaskID IS NOT NULL
  AND sc.DateID IS NOT NULL;


--Total Hours Worked by Each Employee
SELECT 
    de.EmployeeName,
    SUM(ft.Hours) AS TotalHours
FROM [sora].[FactTimeSheet] ft
JOIN [sora].[DimEmployee] de ON ft.EmployeeID = de.EmployeeID
GROUP BY de.EmployeeName
ORDER BY TotalHours DESC;

--Total billable hours
SELECT 
    dp.ProjectName,
    SUM(ft.Hours) AS TotalBillableHours
FROM [sora].[FactTimeSheet] ft
JOIN [sora].[DimProject] dp ON ft.ProjectID = dp.ProjectID
WHERE ft.Billable = 1
GROUP BY dp.ProjectName
ORDER BY TotalBillableHours DESC;

--Average hours worked by employees per day
SELECT 
    de.EmployeeName,
    ROUND(AVG(ft.Hours),2) AS AverageHoursPerDay
FROM [sora].[FactTimeSheet] ft
JOIN [sora].[DimEmployee] de ON ft.EmployeeID = de.EmployeeID
JOIN [sora].[DimDate] dd ON ft.DateID = dd.DateID
GROUP BY de.EmployeeName
ORDER BY AverageHoursPerDay DESC;

--Total hours worked by each task
SELECT 
    dt.TaskName,
    SUM(ft.Hours) AS TotalHours
FROM [sora].[FactTimeSheet] ft
JOIN [sora].[DimTask] dt ON ft.TaskID = dt.TaskID
GROUP BY dt.TaskName
ORDER BY TotalHours DESC;

--Hours worked by each employee on each project
SELECT 
    de.EmployeeName,
    dp.ProjectName,
    SUM(ft.Hours) AS TotalHours
FROM [sora].[FactTimeSheet] ft
JOIN [sora].[DimEmployee] de ON ft.EmployeeID = de.EmployeeID
JOIN [sora].[DimProject] dp ON ft.ProjectID = dp.ProjectID
GROUP BY de.EmployeeName, dp.ProjectName
ORDER BY de.EmployeeName, dp.ProjectName;

