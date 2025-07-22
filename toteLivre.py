from datetime import datetime, timedelta
import json
from metabase import get_dataset, process_data


def generate_tote_pair_zpl(tote1, tote2):
    """
    Generate ZPL code for a pair of tote labels (2 labels per print job)
    """
    zpl_code = (
        "^XA\n"
        "^CF0,40\n"
        f"^FO100,20^FD{tote1}^FS\n"
        f"^FO70,70^BY2,2.5,80^BCN,80,N,N,N^FD{tote1}^FS\n"
        f"^FO500,20^FD{tote2}^FS\n"
        f"^FO470,70^BY2,2.5,80^BCN,80,N,N,N^FD{tote2}^FS\n"
        "^PQ1\n"
        "^XZ"
    )
    return zpl_code


def get_tote_livre(last_print_date=None):
    """
    1. Run dataset with last_print_date as parameter
    2. Get unique_code from each row (already in 'toteXXXXX' format)
    3. Generate ZPL for all available totes, 2 by 2
    """
    try:
        if not last_print_date:
            last_print_date = '2025-07-18T00:00:00'  # Default old date
        print(f"[DEBUG] last_print_date used: {last_print_date}")
        # Ensure only date part is sent to Metabase
        if 'T' in last_print_date:
            last_print_date = last_print_date.split('T')[0]
        elif ' ' in last_print_date:
            last_print_date = last_print_date.split(' ')[0]
        print(f"[DEBUG] last_print_date sent to Metabase: {last_print_date}")
        params = process_data({'data': last_print_date})
        response = get_dataset('11808', params)
        print(f"[DEBUG] Raw response from get_dataset: {response}")

        # The dataset already returns only available totes
        available_totes = []
        if response and isinstance(response, list):
            for row in response:
                if 'unique_code' in row and row['unique_code']:
                    available_totes.append(str(row['unique_code']).strip())

        print(f"Found {len(available_totes)} available totes")

        # Generate ZPL for available totes, 2 by 2
        zpl_list = []
        for i in range(0, len(available_totes), 2):
            tote1 = available_totes[i]
            tote2 = available_totes[i + 1] if i + 1 < len(available_totes) else tote1
            zpl_code = generate_tote_pair_zpl(tote1, tote2)
            zpl_list.append(zpl_code)

        # Save ZPL codes to file
        with open('tote_labels.zpl', 'w', encoding='utf-8') as f:
            for zpl_code in zpl_list:
                f.write(zpl_code + "\n")

        print(f"Generated {len(zpl_list)} ZPL print jobs")
        print(f"Saved to tote_labels.zpl")

        # Create dataset for table display
        dataset = [{"tote_code": tote_code} for tote_code in available_totes]

        return {
            "available_totes": available_totes,
            "zpl_list": zpl_list,
            "total_zpl_jobs": len(zpl_list),
            "dataset": dataset,
            "zpl_data": {
                "zpl_list": zpl_list,
                "total_zpl_jobs": len(zpl_list),
                "total_available_totes": len(available_totes),
                "available_totes": available_totes
            }
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Test the simple function
    result = get_tote_livre()
    
    if "error" not in result:
        print(f"Available totes: {result['available_totes'][:10]}...")  # Show first 10
        print(f"Total ZPL jobs: {result['total_zpl_jobs']}")
    else:
        print(f"Error: {result['error']}")

