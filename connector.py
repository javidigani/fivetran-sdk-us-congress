import requests
import json
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


# Define the update function that Fivetran will call to fetch data
def update(configuration, state):
    """
    Fetches data from the Congress.gov API and yields it to Fivetran.

    Args:
        configuration (dict): Configuration parameters including the API key.
        state (dict): State dictionary to keep track of the last processed record.

    Yields:
        dict: Upsert operation for each record fetched from the API.
    """

    #Initialize state if null
    if not state:
        state = {}
    
    # Base URL for the Congress.gov API
    base_url = "https://api.congress.gov/v3/"

    # Define base endpoints
    endpoints = {
        "bill":{
            "url": base_url + f"bill/{configuration['congress_number']}/",
        }, 
        "congress": {
            "url": base_url + f"congress/{configuration['congress_number']}",
        },
        "member": {
            "url": base_url + f"member/congress/{configuration['congress_number']}",
        },
        "committee": {
            "url": base_url + f"committee/{configuration['congress_number']}",
        }
    }
        

    url = base_url + f"bill/{configuration['congress_number']}/"

    state.setdefault("bill", {})

    current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    state["bill"].setdefault("fromDateTime", None)
    state["bill"]["toDateTime"] = current_timestamp #retrieve latest available each run

    while True:
        # Parameters for the API request
        params = {
            "api_key": configuration["api_key"],  # API key from the configuration
            "congress": configuration["congress_number"], 
            "format": "json", # Data format returned
            "limit": 250,  # Number of records to fetch per request
            "sort": "updateDate+asc", # Sort chronological order

            #Add cursors from state
            "fromDateTime": state["bill"]["fromDateTime"],
            "toDateTime": state["bill"]["toDateTime"],
            "offset": state["bill"].get("offset", 0) # Get offset from state, or set to 0 if empty
        }

        #Verify the endpoint and params
        log.info(url)
        log.info(json.dumps(params))
        log.info(json.dumps(state))

        # Make the API request
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code != 200:
            log.severe(f"API request to {url} failed with status code {response.status_code}")
            state["bill"]["offset"] = 0
            break
        
        # Parse the JSON response
        data = response.json()

        # Check if there are records in the response
        if not data.get("bills"):
            log.info("No more records to fetch.")
            #Next sync continues from latest record
            state["bill"]["fromDateTime"] = current_timestamp 
            #Reset page offset to start fresh
            state["bill"]["offset"] = 0
            yield op.checkpoint(state)
            break
        
        # Process each bill record
        for bill in data["bills"]:
            # Yield an upsert operation to Fivetran
            yield op.upsert(
                table="bills",  # Target table in the destination
                data=bill  # Record data
            )
        
        # Update the offset for the next request
        state["bill"]["offset"] += params["limit"]
        
        # Checkpoint the state to Fivetran
        yield op.checkpoint(state)

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
                "table": "bill_action",
                "primary_key": ["congress","type","number"],
                "columns": {
                    "congress": "INT",
                    "type": "STRING",
                    "number": "STRING",
                    "actionCode": "STRING",
                    "actionDate": "STRING",
                    "actionType": "STRING",
                    "sourceSystem": "JSON",
                    "text": "STRING"
                }
            },
            {
                "table": "bill_amendments",
                "primary_key": ["congress","type","number"],
                "columns": {
                    "congress": "INT",
                    "type": "STRING",
                    "number": "STRING",
                    "description": "STRING",
                    "updateDate": "STRING",
                    "url": "STRING"
                }
            },
            {
                "table": "bill_committees",
                "primary_key": ["congress","type","number", "systemCode"],
                "columns": {
                    "congress": "INT",
                    "type": "STRING",
                    "number": "STRING",
                    "chamber": "STRING",
                    "name": "STRING",
                    "systemCode": "STRING",
                    "committeeType": "STRING",
                    "url": "STRING"
                }
            },
            {
                "table": "bill_cosponsors",
                "primary_key": ["congress","type","number", "bioGuideId"],
                "columns": {
                    "congress": "INT",
                    "type": "STRING",
                    "number": "STRING",
                    "bioGuideId": "STRING"
                }
            },
            {
                "table": "bill_related",
                "primary_key": ["congress","type","number", "related_congress", "related_type","related_number"],
                "columns": {
                    "congress": "INT",
                    "type": "STRING",
                    "number": "STRING",
                    "bioGuideId": "STRING"
                }
            },
        {
            "table": "congress", #TODO NORMALIZE
            "primary_key": ["name"],
            "columns": {
                "name": "STRING",
                "startYear": "STRING",
                "endYear": "STRING",
                "session": "JSON"
            }
        },
        {
            "table": "member",
            "primary_key": ["bioguideId"],
            "columns": {
                "bioguideId": "STRING",
                "depiction": "JSON",
                "district": "STRING",
                "name": "STRING",
                "partyName": "STRING",
                "state": "STRING",
                "terms": "JSON",
                "updateDate": "STRING",
                "url": "STRING"
            }
        },
        {
            "table": "committee",
            "primary_key": ["systemCode"],
            "columns": {
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
        #TODO committee-report
        #TODO committee-print
        #TODO committee_meeting
        #TODO hearing
        #TODO congressional-record
        #TODO daily-congressional-record
        #TODO bound-congressional-record
        #TODO house-communication
        #TODO house-requirement
        #TODO senate-communication
        #TODO senate-requirement
        #TODO nomination
        #TODO treaty
        
    ]

# Initialize the Connector with the update function
connector = Connector(update=update, schema=schema)

# For local debugging
if __name__ == "__main__":
    connector.debug()



"""
,
"related": [
    {
        "name": "detail",
        "endpoint": "{bill_type}/{bill_number}",
        "state": state["bill"]["detail"]
    },
    {
        "name": "actions",
        "endpoint": "{bill_type}/{bill_number}/actions",
        "state": state["bill"]["actions"]
    },
    {
        "name": "amendments",
        "endpoint": "{bill_type}/{bill_number}/amendments",
        "state": state["bill"]["amendments"]
    },
    {
        "name": "committees",
        "endpoint": "{bill_type}/{bill_number}/committees",
        "state": state["bill"]["committees"]
    },
    {
        "name": "cosponsors",
        "endpoint": "{bill_type}/{bill_number}/cosponsors",
        "state": state["bill"]["cosponsors"]
    },
    {
        "name": "related_bills",
        "endpoint": "{bill_type}/{bill_number}/related_bills",
        "state": state["bill"]["related_bills"]
    },
    {
        "name": "subjects",
        "endpoint": "{bill_type}/{bill_number}/subjects",
        "state": state["bill"]["subjects"]
    },
    {
        "name": "summaries",
        "endpoint": "{bill_type}/{bill_number}/summaries",
        "state": state["bill"]["summaries"]
    },
    {
        "name": "text",
        "endpoint": "{bill_type}/{bill_number}/text",
        "state": state["bill"]["text"]
    },
    {
        "name": "titles",
        "endpoint": "{bill_type}/{bill_number}/titles",
        "state": state["bill"]["titles"]
    }
]
"""