import requests

# Define the base URL for the S2DR3 API
BASE_URL = 'https://s2dr3-job-20250428-862134799361.europe-west1.run.app/{USER_ID}/e26bb408-d330-11ef'

# Make the GET request to the S2DR3 API
def get_s2dr3_data(user_id):
    """
    Fetches S2DR3 data from the API for a given user ID.

    Parameters:
    - user_id (str): The user ID to access the S2DR3 API.

    Returns:
    - response (dict): The JSON response from the API.
    """
    url = BASE_URL.format(USER_ID=user_id)

    response = requests.get(url)

    if response.status_code == 200:
        print("Success:", response.json())
    else:
        print("Error:", response.status_code, response.text)
    return response.json() if response.status_code == 200 else None

# Example usage
if __name__ == "__main__":
    user_id = 82712455
    data = get_s2dr3_data(user_id)
    if data:
        print("Data retrieved successfully.")
    else:
        print("Failed to retrieve data.")