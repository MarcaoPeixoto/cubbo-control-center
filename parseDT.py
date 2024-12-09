from datetime import datetime
from dateutil import parser
import os
from dotenv import dotenv_values

env_config = dotenv_values(".env")

date_format = os.environ["DATE_FORMAT"] or env_config.get('DATE_FORMAT')

date_format2 = os.environ["DATE_FORMAT2"] or env_config.get('DATE_FORMAT2')

def parse_date(date_str):
    if date_str is None or date_str == "":
        return None
    try:
        # First try parsing as ISO format with timezone
        return parser.parse(date_str).replace(tzinfo=None)
    except (ValueError, TypeError):
        try:
            # Then try the standard date format
            return datetime.strptime(date_str, date_format)
        except ValueError:
            try:
                # Finally try the alternate date format
                return datetime.strptime(date_str, date_format2)
            except ValueError as e:
                print(f"Warning: Unable to parse date: {date_str}, Error: {e}")
                return None 