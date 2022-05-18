# Ingestion process 

This library is responsable for daily process Covid data from https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/

## Repository

https://github.com/contemglutem/Covid_19_pyspark_json

## Version 
- 1.0 

## Process Description
The data file constains information on the 14-day notification rate of newly reported COVID-19 cases per 100,000 population
and the 14-day notification rate of reported deaths per milion population by day and country. 

The processed database is stored at PostgreSQL by pyspark aplication. PostgreSQL also have a table responsible for control
process of the application, garanting that the spark will start only once a day. 

## IDEs 
- ***PyCHarm (2021.3.1)*** 

## Constants File 
File constats has all variables for PostgreSQL login, it could be located at system variables. This values was setup for 
devolepment and local applications. 

```properties
hostname = 'localhost'
database = 'challenge'
username = 'postgres'
pwd = '132046'
port_id = 5432
driver = 'org.postgresql.Driver'
```
## SQL Querys 
Exercise 3: Create a View
Create a view with the data of the table “Countries of the word” with the latest number of cases,
“Cumulative_number_for_14_days_of_COVID-19_cases_per_100000” and date when the Information
was extracted.

CREATE VIEW Cumulative_number_for_14_days_of_COVID-19_cases_per_100000 AS
SELECT *
FROM covid_data
WHERE year_week = (select max(year_week) from covid_data);



Exercise 4: Queries

--What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
select * from covid_data
where covid_data.indicator = 'cases'
and year_week = '2020-31'
order by rate_14_day desc limit 10 

-- What is the country with the highest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
select * from covid_data
where covid_data.indicator = 'cases'
and year_week = '2020-31'
order by rate_14_day desc limit 1

-- What is the top 10 countries with the highest number of cases among the top 20 richestcountries (by GDP per capita)?
select *, (cumulative_count / population) as qty from covid_data, countries_data
where covid_data.indicator = 'cases'
and year_week = '2022-18'
order by countries_data."GDP ($ per capita)" desc, qty desc limit 20

--List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020
select *, (cumulative_count *1000000/ population) as qty from covid_data, countries_data
where covid_data.indicator = 'cases'
and year_week = '2020-31'
order by countries_data."Pop. Density (per sq. mi.)" desc

--Query the data to find duplicated records
select country, cumulative_count,   covid_data.indicator, count(*)
from covid_data
group by country, cumulative_count, covid_data.indicator
having count(*) > 1
