import requests

PRODUCTS_URL = 'https://fakestoreapi.com/products'

def get_products() -> list[dict]:
    """
    Fetch products from the Fake Store API
    Args:
        None
    Returns:
        list[dict]: The product records
    """
    try:
        response = requests.get(PRODUCTS_URL)
        response.raise_for_status()
        print('API response succeeded')
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Request to %s failed (%s)", PRODUCTS_URL, e)
        raise e