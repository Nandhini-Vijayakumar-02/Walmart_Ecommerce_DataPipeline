Creating a Data Pipeline for the Walmart eCommerce using Pandas and Numpy libraries:

In this project, I am creating a data pipeline for the analysis of demand and supply around the holidays and running preliminary analysis of the data. I will be working with two data sources: grocery sales and complementary data. Lets assume the grocery_sales table in PostgreSQL database and extra_data.parquet file that contains complementary data.

Here is information about all the available columns in the two data files:

"index" - unique ID of the row
"Store_ID" - the store number
"Date" - the week of sales
"Weekly_Sales" - sales for the given store
"IsHoliday" - Whether the week contains a public holiday - 1 if yes, 0 if no.
"Temperature" - Temperature on the day of sale
"Fuel_Price" - Cost of fuel in the region
"CPI" – Prevailing consumer price index
"Unemployment" - The prevailing unemployment rate
"MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4" - number of promotional markdowns
"Dept" - Department Number in each store
"Size" - size of the store
"Type" - type of the store (depends on Size column)

I merged those files for further data manipulations and store the merged file in the clean_data.csv file that should contain the following columns:

"Store_ID"
"Month"
"Dept"
"IsHoliday"
"Weekly_Sales"
"CPI"
"Unemployment"

After merging and cleaning the data, We can analyze monthly sales of Walmart and store the results of your analysis in the agg_date.csv file
