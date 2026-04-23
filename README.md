GitHub version:
# Eco Essentials Data Warehousing Journey

## Project Overview
This project simulates real-world consulting for Eco Essentials, an eco-friendly cookware brand. Eco Essentials aims to optimize its data infrastructure and gain deeper insights into operations and customer behavior. Specifically, Eco Essentials is focusing on the sales process and marketing email conversion. The project covers the entire data lifecycle: data warehouse design, ETL, testing & scheduling, and visualization & communication. 

## Architecture
There are 2 main data sources. Transactional sales data is coming from AWS RDS PostgreSQL, and Salesforce Marketing Cloud email events data is coming from an AWS S3 bucket. The tools used in this project include Fivetran, Snowflake, dbt, Tableau, and GitHub.

## Data Warehouse Design
First, we drafted a bus matrix, outlining key business processes. 

<img width="583" height="158" alt="image" src="https://github.com/user-attachments/assets/0a459d3c-5bdc-4456-b0c4-d0fb56d7e4b3" />


From these business processes, we determined we needed a sales fact table and a marketing events fact table. The sales fact table is made up of one row per product per order and uses the customer, date, product, campaign, and order dimension tables. The marketing events fact table is composed of one row per email event, and requires the email, campaign, customer, date, and eventtype dimension tables. Identifying the date, customer, and campaign dimension tables as conformed allowed us to combine the two separate star schemas into one enterprise data warehouse.

<img width="871" height="525" alt="image" src="https://github.com/user-attachments/assets/e33b25d6-3a59-43b4-aa9b-c6dc714d5397" />

 
## Extract, Transform, Load
We used FiveTran to extract the data from the sources and place the data in an ecoessentials_dw_source schema in Snowflake. We then used dbt to create and populate the dimensional model. To begin, we created a new branch on GitHub and created the ecoessentials folder in the models folder in dbt. We then defined the schema and source .yml files within the ecoessentials folder. From there, we built the dimension and fact tables, using surrogate keys, and placed those in a final schema in Snowflake, dw_ecoessentials. 

<img width="751" height="377" alt="image" src="https://github.com/user-attachments/assets/a932e813-b834-417a-9f35-6d575129e298" />

Example code for dim_product table:
{{ config(
materialized = 'table',
schema = 'dw_ecoessentials'
)
}}

SELECT
  {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
  product_id,
  product_name,
  Product_type
FROM {{ source('ecoessentials_landing', 'product') }}

## Testing & Scheduling
To test the ecoessentials model, 1 of the 4 dbt built-in data tests was added to each dimension and fact table. These tests included unique, not_null, accepted_values, and relationships. Example code for dim_campaign table:
  - name: dim_campaign
    description: "Ecoessentials Campaign Dimension"
    columns: 
      - name: campaign_discount
        tests:
          - accepted_values:
              values:
                - 0.10
                - 0.15
                - 0.20
                - 0.25
                - 0.30
                - 0.35
We then adjusted our FiveTran connections to sync data every 24 hours and created a dbt job to build all of the ecoessential models and scheduled the job to run daily. 

<img width="997" height="461" alt="image" src="https://github.com/user-attachments/assets/e5a1e4ab-b2ac-4450-902e-7c2548ea7365" />


## Data Visualization & Communication
To communicate our data warehouse, we connected our Snowflake database live to Tableau. The goal of this visualization is to help Eco Essentials see trends in sales and marketing email conversions, specifically to see which marketing campaigns have been most successful. 

<img width="623" height="491" alt="image" src="https://github.com/user-attachments/assets/cd83e919-5cc8-4d67-8f42-ec16b3f03b46" />


From this visualization, we can see that the most successful campaign, determined by sales, was the New Customer Welcome with the Welcome to EcoEssentials email. We also filmed a video to present our findings to the Eco Essential leadership: https://drive.google.com/file/d/113jycqbdX9NTE97tVNH-GXpaD564KcRn/view?usp=sharing 

## Summary
Designed a data warehouse for sales and marketing email conversion processes
Performed ETL process on data using FiveTran, Snowflake, and dbt in order to create the data warehouse
Tested models and scheduled the pipeline to run daily
Created a visualization to help Eco Essentials identify trends and successful marketing campaigns

## Takeaways
We optimized the data pipeline for Eco Essentials to identify the most successful campaign and email, being the New Customer Welcome campaign with the Welcome to EcoEssentials email. This campaign and email resulted in over $1500 in sales, with a spike in sales in April. We recommend to Eco Essentials to keep this campaign and email, and model other campaigns after it.

## Team
Anna Giles & Maddy Cook
