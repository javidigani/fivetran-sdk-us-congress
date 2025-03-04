# Congress.gov Fivetran Connector

A Fivetran data connector that syncs data from the Congress.gov API into your data warehouse.

## Overview

This connector fetches data from various Congress.gov API endpoints and loads it into your data warehouse through Fivetran's infrastructure. It supports incremental updates and handles pagination, retries, and state management automatically.

## Supported Data

The connector syncs the following data from Congress.gov:

- Bills
- Amendments 
- Congress Information
- Congressional Members
- Committees
- Hearings
- House Communications
- Senate Communications
- Nominations
- Treaties

## Detail Record Enhancement

The connector supports enriching main records with additional detail data through a configurable detail fetching system. For endpoints that have associated detail records:

1. The main record is fetched first (e.g., a list of members)
2. For each record, a detail URL is constructed using merge fields from the main record
3. A separate API call fetches the detail data
4. The detail data is added to the main record in a `detail` JSON field

Example configuration for member details:

```json
{
    "detail": {
        "url": "member/{bioguideId}",
        "records_key": "member"
    }
}
```

This process allows for:
- Flexible detail data fetching based on configuration
- Efficient batch processing of detail records
- Error handling for failed detail requests
- Storage of detail data in JSON format for flexible querying

## Prerequisites

- Python 3.7+
- A Congress.gov API key from https://api.congress.gov/sign-up
- A Fivetran account

## Configuration

The connector requires the following configuration:

- `api_key`: Your Congress.gov API key
- `starting_congress_number`: The Congress number to start syncing from (e.g. 119 for the 119th Congress)

## State Management

The connector maintains state to enable incremental updates by tracking:
- Last processed Congress number
- Last sync timestamp for each endpoint
- Pagination offsets

## Error Handling

The connector implements:
- Automatic retries for failed API requests (up to 4 attempts)
- Detailed error logging
- Graceful handling of rate limits

## Schema

Each table has a defined schema with appropriate data types and primary keys. See the schema() function in the code for full details of table structures.

## Development

To run the connector locally for development:

```bash
# Install dependencies
pip install -r requirements.txt

# Run in your virtual environment
fivetran debug --configuration configuration.json
```

**For local testing**, you will need to create a `configuration.json` file and add the following key/value pairs: 
1. Your API Key
1. The initial Congress's data you want to retrieve, specified by that Congress's number input as a `STRING` (`"119"` is current as of 2025/02/24):

```
{
    "api_key": "API_KEY",
    "congress_number": "119"
}
```

## Dependencies

- requests>=2.25.1
- fivetran-connector-sdk>=1.0.0
