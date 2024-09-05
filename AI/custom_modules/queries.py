%%writefile queries.py

gdelt_gdp_query = """
with gdp_data AS (
  SELECT
  country_name,
  country_code,
  year,
  value AS gdp
  FROM
    `bigquery-public-data.world_bank_wdi.indicators_data`
  WHERE
    indicator_code = 'NY.GDP.MKTP.CD'
    AND year BETWEEN 1980 AND 2024
  ORDER BY
    country_name,
    year
),
country_codes as (
  select DISTINCT country_code from gdp_data
),
years AS (
  SELECT year
  FROM UNNEST(GENERATE_ARRAY(1980, 2024)) AS year
),
country_year_combinations AS (
  SELECT *    
  FROM country_codes CROSS JOIN years
),
gdp_with_all_years AS (
  SELECT
    cy.country_code,
    cy.year,
    gdp_data.gdp
  FROM
    country_year_combinations AS cy
  LEFT JOIN
    gdp_data
  ON cy.country_code = gdp_data.country_code
  AND cy.year = gdp_data.year
),
gdp_data_with_prev_and_next AS (
  SELECT
    country_code,
    year,
    gdp,
    COALESCE(
      gdp,
      LAST_VALUE(gdp IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS prev_gdp,
    
    COALESCE(
      CASE WHEN gdp IS NOT NULL THEN year END,
      LAST_VALUE(CASE WHEN gdp IS NOT NULL THEN year END IGNORE NULLS
      ) OVER (
        PARTITION BY country_code 
        ORDER BY year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) AS prev_year,

    COALESCE(
      gdp, 
      FIRST_VALUE(gdp IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS next_gdp,

    COALESCE(
      CASE WHEN gdp IS NOT NULL THEN year END,
      FIRST_VALUE(CASE WHEN gdp IS NOT NULL THEN year END IGNORE NULLS
      ) OVER (
        PARTITION BY country_code 
        ORDER BY year 
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      )
    ) AS next_year,
    
  FROM
    gdp_with_all_years
),
interpolated_filled_gdp as (
  select
    country_code, year,
    case
      when gdp is not null then gdp
      when prev_gdp is not null and next_gdp is not null then
        prev_gdp + (next_gdp - prev_gdp) * (year - prev_year) / (next_year - prev_year)
      when prev_gdp is not null then prev_gdp
      when next_gdp is not null then next_gdp
      else 1
      end as gdp
    
    from gdp_data_with_prev_and_next
    
),
global_gdp as (
  select sum(gdp) as total_gdp, year
  from interpolated_filled_gdp
  group by year
),
gdelt_data as (
  SELECT
    case
      when Actor1CountryCode is not null then Actor1CountryCode
      else Actor2CountryCode 
      end AS event_country_code,
    SQLDATE,
    PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS event_date,
    GoldsteinScale,
    AvgTone

  FROM
    `gdelt-bq.full.events`
),
gdelt_with_gdp AS (
  SELECT
    gd.event_country_code,
    gd.SQLDATE,
    gd.event_date,
    gd.GoldsteinScale,
    gd.AvgTone,
    gdp_d.gdp,
    global_gdp.total_gdp
  FROM
    gdelt_data AS gd
  LEFT JOIN
    interpolated_filled_gdp as gdp_d
  ON
    gd.event_country_code = gdp_d.country_code
    AND EXTRACT(YEAR FROM gd.event_date) = gdp_d.year
  LEFT JOIN
    global_gdp
  ON
    EXTRACT(YEAR FROM gd.event_date) = global_gdp.year
)
SELECT
  event_date,
  sum(GoldsteinScale * gdp / total_gdp) as sum_goldstein,
  sum(AvgTone  * gdp / total_gdp) as sum_avgtone,
FROM
  gdelt_with_gdp
where EXTRACT(YEAR FROM event_date) > 1979
group by event_date
order by event_date
"""