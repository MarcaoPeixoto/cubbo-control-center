from datetime import datetime, timedelta
import json
from metabase import get_dataset, process_data


def get_tote_livre():
    try:
        # Call the Metabase query without parameters since it doesn't support the status filter
        response = get_dataset('7371')
        
        return response
    except Exception as e:
        print(f"Error getting tote livre data: {str(e)}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Test the function
    data = get_tote_livre()
    print(f"Found {len(data)} available totes")

