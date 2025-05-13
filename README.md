# Task-1: Big Data Analysis

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: SEHER SANGHANI

*INTERN ID*: CT08DL515

*DOMAIN*: DATA ANALYSIS

*DURATION*: 8 WEEKS

*MENTOR*: NEELA SANTOSH

Big Data Analysis Using PySpark — Project Description

For this project, I explored big data analysis using PySpark, focusing on handling and drawing insights from a large CSV dataset. The goal was to work with a scalable tool that can handle massive datasets efficiently — something traditional Python tools like pandas struggle with as data size grows.

I started by setting up a Spark session using PySpark, which allowed me to work in a distributed environment. This means that instead of processing data on just one machine, Spark can spread the work across multiple cores or even servers — a huge advantage when working with large datasets.

Next, I loaded the dataset using Spark’s CSV reader with automatic schema inference. This helped Spark understand which columns were numeric, strings, or dates, without me manually setting data types. After loading, I looked at the structure and previewed a few rows to get a general feel for the data.

To understand the scale, I counted the total number of records, which is a simple but important step. I then checked for missing or null values in each column. This is crucial because missing data can affect calculations and models later on. I used a combination of PySpark functions like isnull(), isnan(), and when() to count how many missing values existed per column.

After cleaning checks, I generated summary statistics like mean, standard deviation, minimum, and maximum values for the numeric columns. This gave me a better understanding of the data distribution and any potential outliers.

To go a step further, I looked for patterns in the data. For instance, if the dataset had a Category column, I grouped the data by that and counted how many entries were in each group. This helped identify the most common categories. I also calculated the average of a Value column within each category to see which groups had the highest or lowest values — something that could be useful in sales, customer analysis, or performance tracking.

I also included a correlation analysis for numeric columns. This checks how strongly two variables are related — for example, whether an increase in one tends to go along with an increase or decrease in another. This kind of insight is great for spotting trends or deciding which variables might be important for further modeling.

Finally, after all the processing, I closed the Spark session to free up resources.

*Final thoughts*

Overall, this project showed me how powerful PySpark is for working with large datasets. It’s fast, scalable, and great for real-world applications where data volume is too much for local tools. The insights I generated — from missing value counts to group-based averages and correlations — are the kind of things businesses look for when making data-driven decisions. PySpark made it possible to explore all of this, even with a large dataset, without running into memory or performance issues.

*OUTPUT*

![Image](https://github.com/user-attachments/assets/0a55a79a-342c-4dd6-9a99-060e7a9ddd64)
![Image](https://github.com/user-attachments/assets/0674436b-f585-4829-b543-91f74ddcc22a)
