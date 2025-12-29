Select * from swiggy_data;

--Checking for any Null/Empty Values in dataset--
Select
     SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS null_Check_for_state,
     SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_Check_for_City,
     SUM(CASE WHEN Order_Date IS NULL THEN 1 ELSE 0 END) AS null_Check_for_ODate,
     SUM(CASE WHEN Restaurant_Name IS NULL THEN 1 ELSE 0 END) AS null_Check_for_Restro,
     SUM(CASE WHEN Location IS NULL THEN 1 ELSE 0 END) AS null_Check_for_Loc,
     SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_Check_for_Category,
     SUM(CASE WHEN Dish_Name IS NULL THEN 1 ELSE 0 END) AS null_Check_for_Dish,
     SUM(CASE WHEN Price_INR IS NULL THEN 1 ELSE 0 END) AS null_Check_for_INR,
     SUM(CASE WHEN Rating IS NULL THEN 1 ELSE 0 END) AS null_Check_for_Rating,
     SUM(CASE WHEN Rating_Count IS NULL THEN 1 ELSE 0 END) AS null_Check_for_RCount
From swiggy_data;

--Checking for any Empty/Blank Strings--
Select * from swiggy_data
Where State = '' OR City = '' OR  Order_Date = '' OR Restaurant_Name = '' OR Location  = '' OR Dish_Name = '';

--Checking for any duplicate records--
Select State, City, Order_Date, Restaurant_Name, Location, Category, Dish_Name, Price_INR, Rating, Rating_Count, Count(*) As Duplicate_Count
From swiggy_data
Group by State, City, Order_Date, Restaurant_Name, Location, Category, Dish_Name, Price_INR, Rating, Rating_Count 
Having Count(*)>1;

--Removal of dupliaction records whose occurance is more than one using windows function--
WITH CTE AS (
   SELECT *, ROW_NUMBER() OVER 
             (PARTITION BY City, Order_Date, Restaurant_Name, Location, Category, Dish_Name, Price_INR, Rating, Rating_Count
              ORDER BY (SELECT NULL)) AS Duplicate_Count

   FROM swiggy_data
)

DELETE FROM CTE
WHERE Duplicate_Count>1;

--DIMENSIONAL MODELLING (Star Schema) Creation of dimensional table and Fact tables
--Under this next part we have to normalize our dataset by creating it's dimensional modelling for ex : (dim_date, dim_restaurant) and One Fact able
--after once we are done with creating all dimensional tables for each
--we will then start inserting information to each tables one by one with references using Primary key and Foreign key on fact table.

--Creation of Dim_date table
Create table dim_date (
             dates_id INT IDENTITY(1,1) PRIMARY KEY,
             Full_dates DATE,
             Year_Number INT,
             Months INT,
             Month_Name Varchar(20),
             Day_Number INT,
             Quarters INT,
             Weeks INT
        );
 select * from dim_date;

--Creation of dim_location table
Create table dim_location (
             location_id INT IDENTITY(1,1) PRIMARY KEY,
             state Varchar(100),
             city Varchar(100),
             location_name Varchar(100)
        );
 select * from dim_location;

--Creation of dim_restaurant table
Create table dim_restaurant (
             restaurant_id INT IDENTITY(1,1) PRIMARY KEY,
             restaurant_name Varchar(200)
    );
 select * from dim_restaurant;

--Creation of dim_category table
Create table dim_category (
             category_id INT IDENTITY(1,1) PRIMARY KEY,
             category_name Varchar(200)
   );
 select * from dim_category;

--Creation of dim_dishes
Create table dim_dish (
             dish_id INT IDENTITY(1,1) PRIMARY KEY,
             dish_name VARCHAR(200)
   );
 select * from dim_dish;

--Now we will next create 'fact table' which only stores measurable values like price_INR, Rating, Rating_Count, ID's only
Create table swiggy_fact_orders_mesaurable (
              order_id INT IDENTITY(1,1) PRIMARY KEY,
              date_id INT,
              Price_INR Decimal(10,4),
              Rating Decimal(4,2),
              Rating_Count INT,

              location_id INT,
              restaurant_id INT,
              dish_id INT,
              category_id INT

--Below We created a Foreign key to build a relationship b/w Dimension and Fact tables.
              FOREIGN KEY (date_id) REFERENCES dim_date (dates_id),
              FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
              FOREIGN KEY (restaurant_id) REFERENCES dim_restaurant (restaurant_id),
              FOREIGN KEY (category_id) REFERENCES dim_category (category_id),
              FOREIGN KEY (dish_id) REFERENCES dim_dish (dish_id)
  ); 
  Select * from swiggy_fact_orders_mesaurable;

--INSERTINGS VALUES TO DIMENSION TABLES
--INSERTING INFO INTO dim_dates
INSERT INTO dim_date (Full_dates,Year_Number,Months,Month_Name,Day_Number,Quarters,Weeks)
       Select distinct 
                       Order_Date,
                       YEAR(Order_Date),
                       MONTH(Order_Date),
                       DATENAME(Month,Order_Date),
                       DAY(Order_Date),
                       DATEPART(Quarter,Order_Date),
                       DATEPART(Week, Order_DATE)
     FROM swiggy_data
     Where Order_Date IS NOT NULL;

--INSERTING INFO INTO dim_location
INSERT dim_location (state, city, location_name)
    Select distinct
                     State,
                     City,
                     Location
    From swiggy_data;

--INSERTING INFO INTO dim_restaurant
INSERT dim_restaurant (restaurant_name)
    Select distinct 
                      Restaurant_Name
    From swiggy_data;

--INSERTING INFO INTO dim_restaurant
INSERT dim_category (category_name)
    Select distinct 
                    category
    From swiggy_data; 
    
--INSERTING INTO dim_dish
INSERT INTO dim_dish(dish_name)
    Select Distinct
                     Dish_Name
    FROM swiggy_data;

--INSERTING INFORMATIONS INTO swiggy_fact_orders_mesaurable--
--BY USING JOINS

INSERT INTO swiggy_fact_orders_mesaurable (date_id,Price_INR,Rating,Rating_Count,location_id,restaurant_id,category_id,dish_id)
       Select 
             dd.dates_id,
             s.Price_INR,
             s.Rating,
             s.Rating_Count,
             dl.location_id,
             dr.restaurant_id,
             dc.category_id,
             dsh.dish_id
       
       From Swiggy_data s
       JOIN dim_date dd
       ON dd.Full_dates = s.Order_Date

       JOIN dim_location dl
       ON dl.state = s.State
       AND dl.city = s.City
       AND dl.location_name = s.location

       JOIN dim_restaurant dr
       ON dr.restaurant_name = s.Restaurant_Name

       JOIN dim_category dc
       ON dc.category_name = s.Category

       JOIN dim_dish dsh
       ON dsh.dish_name = s.Dish_Name;





--Join Whole Dimension Table with Fact Table--

Select * from swiggy_fact_orders_mesaurable f
Join dim_date d ON f.date_id = d.dates_id
Join dim_location l ON f.location_id = l.location_id
Join dim_restaurant r ON f.restaurant_id = r.restaurant_id
Join dim_category c ON  f.category_id = c.category_id
Join dim_dish dsh ON  f.dish_id = dsh.dish_id; 


--Solving BRD Problems below--
--Solving Business related problems
--KPI's

--Find out total numbers of Orders
Select Count(*) As Total_Orders_count
From swiggy_fact_orders_mesaurable;

--Find out total revenue (INR Million)--
Select FORMAT(SUM(CONVERT(Float,Price_INR))/1000000, 'N2') + ' INR Million'
AS Total_Revenue
From swiggy_fact_orders_mesaurable;

--Find out Average dish price--
Select FORMAT(AVG(CONVERT(Float,Price_INR)), 'N2') + ' INR'
AS Total_Revenue
From swiggy_fact_orders_mesaurable;

--Find out average ratings--
Select Avg(Rating) As Average_Ratings 
from swiggy_fact_orders_mesaurable;  

--Solving GRANULAR REQUIREMENTS--
--DEEP DIVE INTO BUSINESS ANALYSIS--
--DATE BASED ANALYSIS--

--MONTHLY ORDERS TRENDS--
Select d.Year_Number, d.Month_Name, d.Months, Count(*) AS Total_Orders
from swiggy_fact_orders_mesaurable f Join dim_date d 
ON f.date_id = d.dates_id
Group by d.Year_Number, d.Month_Name, d.Months;

--QUATERLY ORDERS TRENDS--
Select e.Year_Number, e.Quarters, Count(*) AS Total_Orders
from swiggy_fact_orders_mesaurable f Join dim_date e
On f.date_id = e.dates_id
Group by e.Year_Number, e.Quarters
ORDER BY Count(*) DESC;

--YEARLY ORDERS TRENDS--
Select e.Year_Number, Count(*) AS Total_Orders
from swiggy_fact_orders_mesaurable f Join dim_date e
ON f.date_id = e.dates_id
Group by e.Year_Number
Order by Count(*) DESC;

--ORDERS TRENDS BY DAY OF WEEKS (Mon-Sun)--
Select 
      DATENAME(WEEKDAY, d.Full_dates) AS Day_Name, Count(*) AS Total_Orders
      From swiggy_fact_orders_mesaurable f JOIN
      dim_date d ON f.date_id = d.dates_id
      Group by DATENAME(WEEKDAY, d.Full_dates), DATEPART(WEEKDAY, d.Full_dates)
      Order by DATEPART(WEEKDAY, d.Full_dates) ASC ;


--FIND OUT TOTAL MONTHLY REVENUES (INR Million)--
Select d.Year_Number, d.Month_Name, d.Months, 
Format(Sum(CONVERT(Float,Price_INR))/1000000, 'N2') + ' INR Million' AS Total_Revenue
from swiggy_fact_orders_mesaurable f Join dim_date d 
ON f.date_id = d.dates_id
Group by d.Year_Number, d.Month_Name, d.Months
ORDER BY Total_Revenue DESC;

--LOCATION-BASED ANALYSIS
--TOP 10 CITIES BY ORDERS VOLUME--
select TOP 10 
l.city, Count(*) AS Total_Orders_Volume_Per_City 
from swiggy_fact_orders_mesaurable f JOIN dim_location l
ON f.location_id = l.location_id
Group by l.city
Order by Total_Orders_Volume_Per_City DESC

--REVENUE CONTRIBUTION BY STATES--
Select l.state, Format(Sum(CONVERT(Float,Price_INR))/1000000, 'N2') + ' INR Million' AS Total_Revenue
FROM swiggy_fact_orders_mesaurable f Join dim_location l
ON f.location_id = l.location_id
Group by l.state
Order by Total_Revenue DESC;


--FOOD PERFORMANCE ANALYSIS
--TOP 10 RESTAURANTS BY ORDERS
select r.restaurant_name, count(*) AS Total_Orders_Per_restaurant
From swiggy_fact_orders_mesaurable f Join dim_restaurant r 
ON f.restaurant_id = r.restaurant_id
Group by restaurant_name
Order by Total_Orders_Per_restaurant DESC;

--Top categories (Indian, Chinese, etc.)
select cat.category_name, count(*) AS Total_Orders_Per_Categories
from swiggy_fact_orders_mesaurable f Join dim_category cat
ON f.category_id = cat.category_id
Group by category_name
Order by Total_Orders_Per_Categories DESC;

--Most ordered dishes
select 
dsh.dish_name, count(*) AS Total_Orders_Per_Dish
from swiggy_fact_orders_mesaurable f Join dim_dish dsh
ON f.dish_id = dsh.dish_id
Group by dish_name
Order by Total_Orders_Per_Dish DESC;

                                      --Cuisine performance → Orders + Avg Rating

select 
       cat.category_name, 
       Count(*) AS Total_Orders, 
       AVG(CONVERT(FLOAT, f.Rating)) AS AVG_Ratings
from swiggy_fact_orders_mesaurable f Join dim_category cat
ON f.category_id = cat.category_id
Group by category_name
Order by AVG_Ratings DESC;


--Customer Spending Insights 
Select
     CASE
         WHEN Price_INR < 100 Then 'Under 100'
         WHEN Price_INR BETWEEN 100 and 199 Then 'Between 100-199'
         WHEN Price_INR BETWEEN 200 and 299 Then 'Between 200-299'
         WHEN Price_INR BETWEEN 300 and 499 Then 'Between 300-499'
         ELSE '500+'
     END AS Price_Range, 
     Count(*) AS Total_Orders
From swiggy_fact_orders_mesaurable
Group by
    CASE
         WHEN Price_INR < 100 Then 'Under 100'
         WHEN Price_INR BETWEEN 100 and 199 Then 'Between 100-199'
         WHEN Price_INR BETWEEN 200 and 299 Then 'Between 200-299'
         WHEN Price_INR BETWEEN 300 and 499 Then 'Between 300-499'
         ELSE '500+'
    END
Order by Total_Orders DESC;

--Rating_Count Distribution (1-5)
Select Rating, Count(*) AS Rating_Counts
From swiggy_fact_orders_mesaurable
group by Rating
Order by Rating_Counts DESC;

select * from swiggy_fact_orders_mesaurable;