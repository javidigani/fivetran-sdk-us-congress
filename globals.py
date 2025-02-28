"""Global configuration variables"""

# Private variable for API key
_api_key = None

# Base URL for Congress.gov API
base_url = "https://api.congress.gov/v3/"

# Endpoint configurations
endpoints = None

def init_globals(configuration):
    """Initialize global variables from configuration"""
    global _api_key, endpoints
    
    # Debug printing
    print("Initializing globals...")
    print(f"Configuration received: {configuration.keys()}")
    
    if "api_key" not in configuration:
        raise ValueError("api_key is missing from configuration")
        
    _api_key = configuration["api_key"]
    
    if not _api_key:
        raise ValueError("api_key is empty in configuration")
    
    print(f"API key set in globals.py: {_api_key[:4]}...")  # Print first 4 chars for verification
    
    # Define endpoint configurations
    endpoints = {
        "bill": {
            "url": base_url + "bill/{congress_number}/",
            "records_key": "bills",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "congress": {
            "url": base_url + "congress/{congress_number}",
            "records_key": "congress",
            "response_type": "object",
            "verbose": False,
            "add_congress_field": False
        },
        "member": {
            "url": base_url + "member/congress/{congress_number}",
            "records_key": "members",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": True,
            "detail": {
                "url": base_url + "member/{bioguideId}",
                "records_key": "member",
                "response_type": "dict"
            }
        },
        "committee": {
            "url": base_url + "committee/{congress_number}",
            "records_key": "committees",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": True
        },
        "amendment": {
            "url": base_url + "amendment/{congress_number}/",
            "records_key": "amendments",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "hearing": {
            "url": base_url + "hearing/{congress_number}/",
            "records_key": "hearings",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "houseCommunication": {
            "url": base_url + "house-communication/{congress_number}/",
            "records_key": "houseCommunications",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "senateCommunication": {
            "url": base_url + "senate-communication/{congress_number}/",
            "records_key": "senateCommunications",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "nomination": {
            "url": base_url + "nomination/{congress_number}/",
            "records_key": "nominations",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        },
        "treaty": {
            "url": base_url + "treaty/{congress_number}/",
            "records_key": "treaties",
            "response_type": "array",
            "verbose": False,
            "add_congress_field": False
        }
    }

def get_endpoint_configs(congress_number):
    """
    Get endpoint configurations with congress number inserted
    
    Args:
        congress_number: Congress number to insert into URL templates
    
    Returns:
        dict: Endpoint configurations with formatted URLs
    """
    formatted_endpoints = {}
    
    for name, config in endpoints.items():
        # Create a new config dict (shallow copy is sufficient)
        formatted_config = config.copy()
        
        # Format the main URL with the congress number
        formatted_config["url"] = config["url"].format(congress_number=congress_number)
        
        formatted_endpoints[name] = formatted_config
    
    return formatted_endpoints

def get_api_key():
    """Get the API key"""
    if not _api_key:
        raise ValueError("API key not initialized. Call init_globals first.")
    return _api_key 
