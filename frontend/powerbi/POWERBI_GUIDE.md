# Power BI Dashboard Guide

Files are in `frontend/powerbi/data/` (created by `build_outputs.py`).

## 1. Load data (Home > Get data > Text/CSV)
`fact_sales_cube.csv`, `fact_orders.csv`, `monthly_sales.csv`, `top_products.csv`,
`category_performance.csv`, `region_performance.csv`, `customer_segments.csv`, `segment_summary.csv`.
Set `month_start` to **Date** type in Power Query.

## 2. Model
Create a Date table (Modeling > New table):
```DAX
DimDate = ADDCOLUMNS(CALENDAR(DATE(2023,1,1), DATE(2025,12,31)),
   "Year", YEAR([Date]), "MonthNo", MONTH([Date]), "Month", FORMAT([Date],"MMM"),
   "MonthStart", DATE(YEAR([Date]), MONTH([Date]), 1))
```
Relationships (many-to-one, single direction): `fact_sales_cube[month_start]` -> `DimDate[MonthStart]`,
`fact_orders[month_start]` -> `DimDate[MonthStart]`.
Make slicer helper tables from `fact_sales_cube`: use its own `category`, `region`, `segment` columns directly,
and relate `fact_orders` to them by creating small dimension tables
(`DimRegion = DISTINCT(fact_sales_cube[region])`, `DimSegment = DISTINCT(fact_sales_cube[segment])`) linked to both facts.
(Mark DimDate as the date table if prompted.)

## 3. DAX measures
```DAX
Total Revenue    = SUM(fact_sales_cube[revenue])
Units Sold       = SUM(fact_sales_cube[units])
Total Orders     = SUM(fact_orders[orders])
Avg Order Value  = DIVIDE(SUM(fact_orders[revenue]), [Total Orders])
Customers        = SUM(segment_summary[customers])
Revenue LY       = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))
YoY Growth %     = DIVIDE([Total Revenue] - [Revenue LY], [Revenue LY])
Revenue MoM %    = VAR p = CALCULATE([Total Revenue], DATEADD(DimDate[Date], -1, MONTH))
                   RETURN DIVIDE([Total Revenue] - p, p)
```

## 4. Report page layout
| Visual | Fields |
|---|---|
| 5 KPI cards | Total Revenue, Units Sold, Total Orders, Avg Order Value, Customers |
| Line chart | Axis `DimDate[MonthStart]`, Values `[Total Revenue]` |
| Clustered column (monthly comparison) | Axis `DimDate[Month]` (sort by MonthNo), Legend `DimDate[Year]`, Values `[Total Revenue]` |
| Bar chart | Axis `category`, Values `[Total Revenue]` |
| Table / bar | `product_name`, `[Total Revenue]`, `[Units Sold]` with Top N = 10 filter |
| Donut | Legend `segment`, Values `[Total Revenue]` |
| Filled map / bar | `region`, `[Total Revenue]` |
| Slicers | `DimDate[Date]` (Between), `category`, `segment`, `region` |

Use **View > Sync slicers** so all four slicers filter every visual. Save as `ecommerce_dashboard.pbix`.
