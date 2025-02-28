import requests
import json
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import time

from globals import (
    base_url,
    init_globals,
    get_endpoint_configs,
    get_api_key
)


def get_detail_data(url, record, verbose=False):
    """
    Fetch detail data for a record.
    
    Args:
        url (str): URL template with merge fields
        record (dict): Current record containing merge field values
        verbose (bool): Enable verbose logging
    
    Returns:
        dict: Detail data or error information
    """
    try:
        # Replace merge fields in URL
        formatted_url = url
        for key, value in record.items():
            formatted_url = formatted_url.replace(f"{{{key}}}", str(value))
            
        if verbose:
            log.info(f"Requesting detail data from: {formatted_url}")
            
        response = requests.get(formatted_url, params={"api_key": get_api_key(), "format": "json"})
        
        if response.status_code != 200:
            return {
                "url": formatted_url,
                "error": f"API request failed with status {response.status_code}",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
        return response.json()
        
    except Exception as e:
        return {
            "url": formatted_url if 'formatted_url' in locals() else url,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

def fetch_endpoint_data(endpoint_url, endpoint_name, records_key, response_type, state, congress_number, verbose=False, add_congress_field=False, detail_config=None):
    """
    Generic function to fetch data from any Congress.gov API endpoint.

    Args:
        endpoint_url (str): The base URL for the endpoint
        endpoint_name (str): Name of the endpoint (used for state management)
        records_key (str): Key or dot-notation path to access records in response
        response_type (str): Type of the response (array or object)
        state (dict): State dictionary to track progress
        congress_number (int): The current congress number
        verbose (bool): Whether to output detailed debug logs for this endpoint
        add_congress_field (bool): Whether to add the congress number to the record
        detail_config (dict): Configuration for fetching detail data

    Yields:
        dict: Upsert operations for records fetched from the API
    """
    state.setdefault(endpoint_name, {})
    current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Check if we need to process this endpoint based on state
    state_congress = state[endpoint_name].get("congress_number")
    state_to_datetime = state[endpoint_name].get("toDateTime")
    
    if state_congress and state_to_datetime:
        if congress_number < state_congress:
            log.info(f"Skipping {endpoint_name} for congress {congress_number} - already processed up to congress {state_congress}")
            return
        elif congress_number == state_congress and current_timestamp <= state_to_datetime:
            log.info(f"Skipping {endpoint_name} for congress {congress_number} - no new data since {state_to_datetime}")
            return

    # Base parameters for all requests
    params = {
        "api_key": get_api_key(),
        "congress": congress_number,
        "format": "json"
    }

    # Add pagination parameters for array-type endpoints
    if response_type == "array":
        state[endpoint_name].setdefault("fromDateTime", None)
        state[endpoint_name].setdefault("offset", 0)
        state[endpoint_name].setdefault("congress_number", congress_number)
        state[endpoint_name]["toDateTime"] = current_timestamp
        params.update({
            "limit": 50 if detail_config else 250,
            "sort": "updateDate+asc",
            "fromDateTime": state[endpoint_name]["fromDateTime"],
            "toDateTime": state[endpoint_name]["toDateTime"],
            "offset": state[endpoint_name].get("offset", 0)
        })

    while True:
        log.info(f"Requesting {endpoint_name} data...")
        if verbose:
            log.info(f"Full URL: {endpoint_url}")
            log.info(f"Parameters: {params}")
        
        # Add retry logic
        retry_count = 0
        max_retries = 4
        while retry_count < max_retries:
            response = requests.get(endpoint_url, params=params)
            
            if response.status_code != 200:
                retry_count += 1
                if retry_count == max_retries:
                    log.severe(f"API request to {endpoint_url} failed {max_retries} times with status code {response.status_code}. Stopping endpoint processing.")
                    return  # Exit without updating state
                else:
                    log.info(f"API request failed with status {response.status_code}. Attempt {retry_count} of {max_retries}. Retrying in 2 seconds...")
                    time.sleep(2)
                    continue
            break  # Success - exit retry loop
        
        if retry_count == max_retries:
            break  # Exit main processing loop
        
        data = response.json()
        
        # Check for pagination metadata if available
        pagination_info = data.get("pagination", {})
        total_records = pagination_info.get("count")
        if total_records is not None and verbose:
            log.info(f"Total available records for {endpoint_name}: {total_records}")
        
        # Handle nested JSON paths using dot notation
        current_data = data
        if verbose:
            log.info(f"Initial response for {endpoint_name}: {current_data}")
        for key in records_key.split('.'):
            current_data = current_data.get(key, {})
            if verbose:
                log.info(f"After getting key '{key}': {current_data}")
        
        if not current_data:
            log.info(f"No more records to fetch for {endpoint_name}.")
            if response_type == "array":
                state[endpoint_name]["fromDateTime"] = current_timestamp
                state[endpoint_name]["offset"] = 0
                state[endpoint_name]["congress_number"] = congress_number
            yield op.checkpoint(state)
            break

        # Process the response based on its type
        if isinstance(current_data, list):
            if verbose:
                log.info(f"Processing array response for {endpoint_name} with {len(current_data)} records")
            try:
                # Add congress field if configured
                if add_congress_field:
                    for record in current_data:
                        record["congress"] = congress_number
                
                # Fetch detail data if configured
                if detail_config:
                    log.info(f"Fetching detail data for {len(current_data)} {endpoint_name} records")
                    for record in current_data:
                        detail_data = get_detail_data(
                            detail_config["url"],
                            record,
                            verbose
                        )
                        
                        if "error" in detail_data:
                            record["detail"] = detail_data
                        else:
                            # Navigate to the specified records_key in detail response
                            detail_result = detail_data
                            for key in detail_config["records_key"].split('.'):
                                detail_result = detail_result.get(key, {})
                            record["detail"] = detail_result
                
                # Insert enhanced records
                for record in current_data:
                    yield op.upsert(
                        table=endpoint_name,
                        data=record
                    )
                log.info(f"Successfully processed {len(current_data)} records for {endpoint_name}")
            except Exception as e:
                log.severe(f"Failed to process {endpoint_name} for congress {congress_number}: {str(e)}")
                yield op.checkpoint(state)
                return
            
            if response_type == "array":
                next_offset = state[endpoint_name]["offset"] + params["limit"]
                # Check if we would exceed the total available records
                if total_records is not None and next_offset >= total_records:
                    log.info(f"Reached total record count ({total_records}) for {endpoint_name}")
                    state[endpoint_name]["fromDateTime"] = current_timestamp
                    state[endpoint_name]["offset"] = 0
                    state[endpoint_name]["congress_number"] = congress_number
                    yield op.checkpoint(state)
                    break
                
                state[endpoint_name]["offset"] = next_offset
                state[endpoint_name]["congress_number"] = congress_number
                params["offset"] = next_offset
                if verbose:
                    log.info(f"Updated offset for {endpoint_name}: {next_offset}")
                yield op.checkpoint(state)
            else:
                break
                
        elif isinstance(current_data, dict):
            if verbose:
                log.info(f"Processing single object response for {endpoint_name}")
            try:
                if add_congress_field:
                    current_data["congress"] = congress_number
                
                # Fetch detail data if configured
                if detail_config:
                    detail_data = get_detail_data(
                        detail_config["url"],
                        current_data,
                        verbose
                    )
                    
                    if "error" in detail_data:
                        current_data["detail"] = detail_data
                    else:
                        # Navigate to the specified records_key in detail response
                        detail_result = detail_data
                        for key in detail_config["records_key"].split('.'):
                            detail_result = detail_result.get(key, {})
                        current_data["detail"] = detail_result
                
                yield op.upsert(
                    table=endpoint_name,
                    data=current_data
                )
                log.info(f"Successfully processed single record for {endpoint_name}")
            except Exception as e:
                log.severe(f"Failed to process {endpoint_name} for congress {congress_number}: {str(e)}")
                yield op.checkpoint(state)
                return
            
            state[endpoint_name]["congress_number"] = congress_number
            yield op.checkpoint(state)
            break
        else:
            log.severe(f"Unexpected data type for {endpoint_name}: {type(current_data)}")
            raise ValueError(f"Unexpected data type in response: {type(current_data)}")

def get_current_congress():
    """
    Fetch the current congress number from the API.
    """
    print(f"API key in get_current_congress: {get_api_key()[:4]}...")  # Debug print
    
    endpoint_url = base_url + "congress/current"
    
    response = requests.get(endpoint_url, params={"api_key": get_api_key(), "format": "json"})
    
    if response.status_code == 403:
        error_msg = (
            f"API Key authentication failed (403). "
            f"Response: {response.text if response.text else 'No response body'}. "
            f"URL: {endpoint_url}"
        )
        log.severe(error_msg)
        raise SystemExit(error_msg)
    
    if response.status_code != 200:
        error_msg = (
            f"Failed to get current congress number. "
            f"Status code: {response.status_code}. "
            f"Response: {response.text if response.text else 'No response body'}. "
            f"URL: {endpoint_url}"
        )
        log.severe(error_msg)
        raise ValueError(error_msg)
    
    data = response.json()
    current_congress = data.get("congress", {}).get("number")
    
    if not current_congress:
        error_msg = (
            f"Congress number not found in response. "
            f"Response data: {data}"
        )
        log.severe(error_msg)
        raise ValueError(error_msg)
    
    log.info(f"Current congress number: {current_congress}")
    return current_congress

def update(configuration, state):
    """
    Fetches data from all configured Congress.gov API endpoints.
    """
    if not state:
        state = {}
    
    # Initialize global variables
    init_globals(configuration)
    
    # Get current congress number
    current_congress_number = get_current_congress()
    starting_congress = int(configuration['starting_congress_number'])
    
    for congress_number in range(starting_congress, current_congress_number + 1):
        log.info(f"Processing data for Congress {congress_number}")
        
        endpoints = get_endpoint_configs(congress_number)

        for endpoint_name, endpoint_config in endpoints.items():
            log.info(f"Processing endpoint: {endpoint_name} for Congress {congress_number}")
            yield from fetch_endpoint_data(
                endpoint_config["url"],
                endpoint_name,
                endpoint_config["records_key"],
                endpoint_config["response_type"],
                state,
                congress_number,
                endpoint_config.get("verbose", False),
                endpoint_config.get("add_congress_field", False),
                endpoint_config.get("detail")
            )

def schema(configuration: dict):
    return [
        {
            "table": "bill", # Name of the table in the destination
            "primary_key": ["congress","type","number"], # Primary key column(s) for the table
            "columns": { #Define the columns and their data types
                "congress": "INT",
                "latestAction": "JSON",
                "number": "STRING",
                "originChamber": "STRING",
                "originChamberCode": "STRING",
                "title": "STRING",
                "type": "STRING",
                "updateDate": "STRING", #"NAIVE_DATE",
                "updateDateIncludingText": "STRING", #"UTC_DATETIME",
                "url": "STRING"
            }
        },
        {
            "table": "amendment",
            "primary_key": ["congress","type","number"],
            "columns": {
                "congress": "INT",
                "type": "STRING",
                "number": "STRING",
                "purpose": "JSON",
                "latestAction": "JSON",
                "url": "STRING"
            }
        },
        {
            "table": "congress", #TODO NORMALIZE
            "primary_key": ["number"],
            "columns": {
                "number": "INT",
                "name": "STRING",
                "startYear": "STRING",
                "endYear": "STRING",
                "sessions": "JSON"
            }
        },
        {
            "table": "member", #TODO NORMALIZE
            "primary_key": ["bioguideId"],
            "columns": {
                "congress": "INT",
                "bioguideId": "STRING",
                "depiction": "JSON",
                "district": "STRING",
                "name": "STRING",
                "partyName": "STRING",
                "state": "STRING",
                "terms": "JSON",
                "updateDate": "STRING",
                "url": "STRING",
                "detail": "JSON"
            }
        },
        {
            "table": "committee", #TODO NORMALIZE
            "primary_key": ["systemCode"],
            "columns": {
                "congress": "INT",
                "chamber": "STRING",
                "committeeTypeCode": "STRING",
                "updateDate": "STRING",
                "name": "STRING",
                "parent": "STRING",
                "subcommittees": "JSON",
                "systemCode": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "hearing",
            "primary_key": ["congress","chamber","jacketNumber"],
            "columns": {
                "congress": "INT",
                "chamber": "STRING",
                "jacketNumber": "INT",
                "updateDate": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "house_communication",
            "primary_key": [], # Need to handle null value in "number" column
            "columns": {
                "congressNumber": "INT",
                "chamber": "STRING",
                "number": "STRING",
                "communicationType": "JSON",
                "reportNature": "STRING",
                "submittingAgency": "STRING",
                "submittingOfficial": "STRING",
                "updateDate": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "senate_communication",
            "primary_key": ["congress","chamber","number"],
            "columns": {
                "congress": "INT",
                "chamber": "STRING",
                "number": "INT",
                "communicationType": "JSON",
                "updateDate": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "nomination",
            "primary_key": ["congress","number"],
            "columns": {
                "congress": "INT",
                "number": "INT",
                "citation": "STRING",
                "organization": "STRING",
                "partNumber": "STRING",
                "nominationType": "JSON",
                "receivedDate": "STRING",
                "latestAction": "JSON",
                "updateDate": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "treaty",
            "primary_key": ["congressReceived","number"],
            "columns": {
                "congressReceived": "INT",
                "congressConsidered": "INT",
                "number": "INT",
                "parts": "JSON",
                "suffix": "STRING",
                "topic": "STRING",
                "transmittedDate": "STRING",
                "updateDate": "STRING",
                "url": "STRING"
            }
        }
        
    ]

# Initialize the Connector with the update function
connector = Connector(update=update, schema=schema)

# For local debugging
if __name__ == "__main__":
    connector.debug()
