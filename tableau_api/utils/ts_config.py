import json
from tableau_api_lib import sample_config

__all__ = ["generate_config"]

API_VERSION = "3.15"
SERVERS = {"tableau_prod": "PROD_SERVER_URL", "tableau_sim": "SIM_SERVER_URL"}
SITES = {
    "SITE_NAME1": {
        "api_version": API_VERSION,
        'personal_access_token_name': '<YOUR_USERNAME>',
        'personal_access_token_secret': '<YOUR_PASSWORD>',
        "site_name": "SITE_NAME1",
        "site_url": "SITE_NAME1" 
    }, 
    "SITE_NAME2": {
        "api_version": API_VERSION,
        'personal_access_token_name': '<YOUR_USERNAME>',
        'personal_access_token_secret': '<YOUR_PASSWORD>',
        "site_name": "SITE_NAME2",
        "site_url": "SITE_NAME2"
    },
    "SITE_NAME3": {
        "api_version": API_VERSION,
        'personal_access_token_name': '<YOUR_USERNAME>',
        'personal_access_token_secret': '<YOUR_PASSWORD>',
        "site_name": "SITE_NAME3",
        "site_url": "SITE_NAME3"
    }
}

def generate_config():
    for env, server in SERVERS.items():
        for site_name in SITES:
            temp = {"server": server}
            temp.update(SITES[site_name])
            sample_config[f"{env}_{site_name}"] = temp
    del sample_config["tableau_prod"]
    with open('ts_config.json', 'w') as f:
        json.dump(sample_config, f, indent=4)
    
    return sample_config