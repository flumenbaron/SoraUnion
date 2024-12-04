--Unrefactored script
SELECT
    c.Name,
    f.Role,
    SUM(c.hours) AS Total_Tracked_Hours,
    SUM(f.EstimatedHours) AS Total_Allocated_Hours,
    c.TaskDate
FROM sora.stgClick c
JOIN sora.stgFloat f on c.Name = f.Name
GROUP BY c.Name, f.Role
HAVING SUM(c.hours) > 100
ORDER BY Total_Allocated_Hours DESC;

--Refactored script

-- Create an index on the Name column in the ClickUp table
CREATE INDEX IX_ClickUp_Name ON ClickUp(Name);

-- Create an index on the Name column in the Float table
CREATE INDEX IX_Float_Name ON Float(Name);

SELECT 
    c.Name,
    f.Role,
    SUM(c.hours) AS Total_Tracked_Hours,
    SUM(f.EstimatedHours) AS Total_Allocated_Hours,
    c.TaskDate
    --CAST(MAX(c.TaskDate) AS DATE) AS Last_Tracked_Date
FROM  sora.stgClick c
JOIN  sora.stgFloat f ON c.Name = f.Name
GROUP BY  c.Name, f.Role, c.TaskDate
HAVING  SUM(c.hours) > 100
ORDER BY Total_Allocated_Hours DESC;

