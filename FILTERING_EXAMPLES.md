# CQC Data Filtering API Examples

## Base URL
`GET /api/v1/filter/filter-data`

## Authentication
No authentication required for these endpoints.

## Available Endpoints

### 1. Filter Data - `GET /api/v1/filter/filter-data`
Main endpoint for filtering CQC data with flexible query parameters.

### 2. Available Columns - `GET /api/v1/filter/available-columns`
Get list of all columns available for filtering.

### 3. Available Operators - `GET /api/v1/filter/operators`
Get list of supported filter operators.

## Query Parameters

### Common Parameters
- `limit`: Maximum records to return (1-1000, default: 100)
- `offset`: Records to skip for pagination (default: 0)
- `order_by`: Column to sort by (any available column)
- `order_direction`: Sort direction (`ASC` or `DESC`, default: ASC)
- `logic`: Logic between conditions (`AND` or `OR`, default: AND)

### Simple Filter Parameters
- `location_id`: Filter by location ID (exact match)
- `location_name`: Filter by location name (contains)
- `location_city`: Filter by city (exact match)
- `location_region`: Filter by region (exact match, or comma-separated for multiple)
- `location_postal_code`: Filter by postal code (exact match)
- `provider_name`: Filter by provider name (contains)
- `is_care_home`: Filter by care home status (`Y` or `N`)
- `is_dormant`: Filter by dormant status (`Y` or `N`)
- `latest_overall_rating`: Filter by rating (exact or comma-separated list)
- `care_homes_beds_min`: Minimum number of beds
- `care_homes_beds_max`: Maximum number of beds
- `year`: Filter by year
- `month`: Filter by month (1-12)

### Boolean Service Flags
- `domiciliary_care`: Filter domiciliary care (`true`/`false`)
- `care_home_nursing`: Filter nursing care homes (`true`/`false`)
- `care_home_without_nursing`: Filter care homes without nursing (`true`/`false`)
- `hospital_services_acute`: Filter acute hospital services (`true`/`false`)
- `community_health_care`: Filter community health care (`true`/`false`)
- `older_people_65_plus`: Filter elderly services (`true`/`false`)
- `dementia`: Filter dementia care (`true`/`false`)
- `mental_health_needs`: Filter mental health services (`true`/`false`)
- `learning_disabilities_autistic`: Filter learning disability/autism services (`true`/`false`)
- `physical_disability`: Filter physical disability services (`true`/`false`)

### Advanced JSON Filter
- `filters`: JSON string for complex filtering (see examples below)

## Example Requests

### Example 1: Simple Query - Care Homes in London with Good Rating

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?is_care_home=Y&location_city=London&latest_overall_rating=Good&limit=50&order_by=location_name&order_direction=ASC"
```

### Example 2: Multiple Values - Outstanding or Good Rated Locations

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?latest_overall_rating=Outstanding,Good&limit=100&order_by=publication_date&order_direction=DESC"
```

### Example 3: Boolean Flags - Domiciliary Care in Multiple Regions

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?domiciliary_care=true&location_region=London,South%20East&limit=200"
```

### Example 4: Numeric Ranges - Large Care Homes with Mental Health

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?is_care_home=Y&care_homes_beds_min=50&mental_health_needs=true&limit=25"
```

### Example 5: Text Search - Providers with "Healthcare" in Name

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?provider_name=Healthcare&limit=100&order_by=provider_name"
```

### Example 6: Time Period - Recent Data with Service Types

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?year=2024&hospital_services_acute=true&community_health_care=true&limit=75"
```

### Example 7: Bed Range - Medium to Large Care Homes

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?care_homes_beds_min=20&care_homes_beds_max=100&is_care_home=Y&limit=50"
```

### Example 8: Complex Boolean Logic - Elderly Care with Dementia (OR logic)

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?older_people_65_plus=true&dementia=true&logic=OR&limit=30"
```

### Example 9: Pagination - Second Page of Results

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?location_region=London&limit=50&offset=50"
```

### Example 10: Multiple Service Types

```bash
curl "http://localhost:8000/api/v1/filter/filter-data?care_home_nursing=true&mental_health_needs=true&physical_disability=true&logic=AND"
```

## Advanced JSON Filtering Examples

For complex queries that can't be expressed with simple parameters, use the `filters` parameter with JSON:

### Example 11: Contains Search with Case Sensitivity

```bash
curl "http://localhost:8000/api/v1/filter/filter-data" \
  --data-urlencode 'filters=[{"column":"provider_name","value":"Healthcare","operator":"contains","case_sensitive":false}]'
```

### Example 12: Geographic Bounding Box

```bash
curl "http://localhost:8000/api/v1/filter/filter-data" \
  --data-urlencode 'filters=[
    {"column":"location_latitude","value":51.4,"operator":"gte"},
    {"column":"location_latitude","value":51.6,"operator":"lte"},
    {"column":"location_longitude","value":-0.2,"operator":"gte"},
    {"column":"location_longitude","value":0.1,"operator":"lte"}
  ]&logic=AND'
```

### Example 13: Exclude Specific Ratings

```bash
curl "http://localhost:8000/api/v1/filter/filter-data" \
  --data-urlencode 'filters=[{"column":"latest_overall_rating","value":"Inadequate,Requires improvement","operator":"not_in"}]'
```

### Example 14: Date Range with Month Filter

```bash
curl "http://localhost:8000/api/v1/filter/filter-data" \
  --data-urlencode 'filters=[
    {"column":"year","value":2023,"operator":"gte"},
    {"column":"month","value":"6,7,8","operator":"in"}
  ]&logic=AND'
```

### Example 15: Name Starts With Pattern

```bash
curl "http://localhost:8000/api/v1/filter/filter-data" \
  --data-urlencode 'filters=[{"column":"location_name","value":"St ","operator":"starts_with","case_sensitive":false}]'
```

## Response Format

```json
{
  "status": "success",
  "data": [
    {
      "location_id": "1-1000587219",
      "location_name": "Example Care Home",
      "provider_name": "Example Healthcare Provider",
      "latest_overall_rating": "Good",
      "is_care_home": "Y",
      "care_homes_beds": 45,
      "location_city": "London",
      "location_region": "London",
      "year": 2024,
      "month": 8,
      // ... all other fields
    }
  ],
  "pagination": {
    "total_count": 1250,
    "limit": 50,
    "offset": 0,
    "current_page": 1,
    "total_pages": 25
  },
  "filters_applied": {
    "conditions": 3,
    "logic": "AND"
  }
}
```

## Column Categories

### Location Information
- `location_id`, `location_name`, `location_ods_code`
- `location_street_address`, `location_city`, `location_county`, `location_postal_code`
- `location_region`, `location_nhs_region`, `location_local_authority`
- `location_latitude`, `location_longitude`
- `location_telephone_number`, `location_parliamentary_constituency`

### Provider Information
- `provider_id`, `provider_name`, `provider_type_sector`
- `provider_street_address`, `provider_city`, `provider_county`
- `ownership_type`, `companies_house_number`
- `nominated_individual_name`, `main_partner_name`

### Care Information
- `is_care_home`, `care_homes_beds`, `registered_manager`
- `latest_overall_rating`, `publication_date`, `is_inherited_rating`
- `is_dormant`, `location_type_sector`

### Activity Flags (Boolean)
- `accommodation_nursing_personal_care`
- `treatment_disease_disorder_injury`
- `care_home_nursing`, `care_home_without_nursing`
- `domiciliary_care`, `hospital_services_acute`
- `community_health_care`

### Service User Bands (Boolean)
- `children_0_3_years`, `children_4_12_years`, `children_13_18_years`
- `adults_18_65_years`, `older_people_65_plus`
- `dementia`, `learning_disabilities_autistic`
- `mental_health_needs`, `physical_disability`

### Time Period
- `year`, `month`, `file_name`

## Error Handling

### Invalid Column
```json
{
  "detail": "Column 'invalid_column' not available for filtering"
}
```

### Invalid Operator
```json
{
  "detail": "Operator 'invalid_op' not supported"
}
```

### No Filter Conditions
```json
{
  "detail": "At least one filter condition is required"
}
```

## Performance Tips

1. **Use specific filters** - More specific filters reduce result set size
2. **Limit results** - Use reasonable limits (default: 100, max recommended: 1000)
3. **Index-friendly columns** - `location_id`, `provider_id`, `year`, `month` are indexed
4. **Combine filters** - Use AND logic for better performance than OR
5. **Pagination** - Use offset/limit for large result sets

## Getting Available Options

### Get All Available Columns
```bash
curl -X GET "http://localhost:8000/api/v1/filter/available-columns"
```

### Get Available Operators
```bash
curl -X GET "http://localhost:8000/api/v1/filter/operators"
```

## Use Cases

1. **Regulatory Compliance** - Find locations with specific ratings or inspection status
2. **Market Analysis** - Analyze care home capacity and distribution
3. **Service Planning** - Identify service gaps in specific regions
4. **Quality Monitoring** - Track changes in ratings over time
5. **Research** - Extract datasets for academic or policy research
6. **Business Intelligence** - Competitive analysis and market mapping