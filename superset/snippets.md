# Superset SQL Snippets

This file collects copy-paste SQL snippets for common chart customizations in Superset.

Dataset used in examples:
- `mart.v_air_quality_hourly`

How to use these:
- Add expressions in **Dataset -> Edit dataset -> Columns -> + COLUMN** for calculated columns.
- Add expressions in **Dataset -> Edit dataset -> Metrics -> + METRIC** for custom metrics.
- Use the output alias as chart dimension/metric fields.
- Prefer numeric sort columns (`month_number`, `day_of_week_number`) when a chart type supports custom sorting.

## Month Names In Logical Order (Space-Padded Label)

Use this as a calculated column when a chart/pivot only supports alphabetic sorting on labels.

```sql
CASE
WHEN month_name = 'January'   THEN '           Jan'
WHEN month_name = 'February'  THEN '          Feb'
WHEN month_name = 'March'     THEN '         Mar'
WHEN month_name = 'April'     THEN '        Apr'
WHEN month_name = 'May'       THEN '       May'
WHEN month_name = 'June'      THEN '      Jun'
WHEN month_name = 'July'      THEN '     Jul'
WHEN month_name = 'August'    THEN '    Aug'
WHEN month_name = 'September' THEN '   Sep'
WHEN month_name = 'October'   THEN '  Oct'
WHEN month_name = 'November'  THEN ' Nov'
WHEN month_name = 'December'  THEN 'Dec'
ELSE month_name
END
```

## Month Labels Without Space Padding (Preferred Where Supported)

If the visualization supports sort-by-column, use:
- label: `month_name`
- sort by: `month_number` ascending

If you still need a single expression for display + ordering:

```sql
CONCAT(LPAD(month_number::text, 2, '0'), ' - ', month_name)
```

## Season names (in alphabetic order - use spaces to adjust)

```sql
CASE 
  WHEN month_name IN ('December', 'January', 'February') THEN 'Winter'
  WHEN month_name IN ('March', 'April', 'May') THEN 'Spring'
  WHEN month_name IN ('June', 'July', 'August') THEN 'Summer'
  WHEN month_name IN ('September', 'October', 'November') THEN 'Autumn'
  ELSE NULL 
END
```

## Weekday Names In Logical Order (Space-Padded Label)

Use this as a calculated column when weekday labels would otherwise sort alphabetically.

```sql
CASE
WHEN day_name = 'Mon' THEN '      Mon'
WHEN day_name = 'Tue' THEN '     Tue'
WHEN day_name = 'Wed' THEN '    Wed'
WHEN day_name = 'Thu' THEN '   Thu'
WHEN day_name = 'Fri' THEN '  Fri'
WHEN day_name = 'Sat' THEN ' Sat'
WHEN day_name = 'Sun' THEN 'Sun'
ELSE day_name
END
```

## Weekday Labels Without Space Padding (Preferred Where Supported)

If the visualization supports sort-by-column, use:
- label: `day_name`
- sort by: `day_of_week_number` ascending

If you need one expression that carries sort intent into the label:

```sql
CONCAT(day_of_week_number::text, ' - ', day_name)
```

## Wind Direction Metrics (8-Sector Radar)

Use these as custom metrics for charts where each metric represents one wind direction.

```sql
AVG(CASE WHEN wind_sector = 'N'  THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'NE' THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'E'  THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'SE' THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'S'  THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'SW' THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'W'  THEN ws10 ELSE NULL END)
AVG(CASE WHEN wind_sector = 'NW' THEN ws10 ELSE NULL END)
```

Suggested metric names in Superset:
- `ws10_avg_n`
- `ws10_avg_ne`
- `ws10_avg_e`
- `ws10_avg_se`
- `ws10_avg_s`
- `ws10_avg_sw`
- `ws10_avg_w`
- `ws10_avg_nw`

## Simple Data-Quality Filters

Use these as chart-level SQL filters when needed:

Keep rows where wind speed is present:

```sql
ws10 IS NOT NULL
```

Keep rows with valid wind direction degree:

```sql
wd10 BETWEEN 0 AND 360
```

Keep rows where PM2.5 and PM10 are both present:

```sql
pm2_5 IS NOT NULL AND pm10 IS NOT NULL
```

## Humidity And Rainfall Filters (Station 19)

Keep rows where humidity is present:

```sql
hum IS NOT NULL
```

Keep rows where rainfall/downfall is measured:

```sql
rain IS NOT NULL
```
