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
        f"^FO100,20^FDtote{tote1}^FS\n"
        f"^FO70,70^BY2,2.5,80^BCN,80,N,N,N^FDtote{tote1}^FS\n"
        f"^FO500,20^FDtote{tote2}^FS\n"
        f"^FO470,70^BY2,2.5,80^BCN,80,N,N,N^FDtote{tote2}^FS\n"
        "^PQ1\n"
        "^XZ"
    )
    return zpl_code


def get_tote_livre():
    """
    Simple function:
    1. Run dataset
    2. Get unique_code from each row
    3. Exclude first 4 letters (tote part)
    4. Get the number (1-5000)
    5. Create list of excluded numbers
    6. Loop 1-5000 excluding those numbers
    7. Generate ZPL for remaining numbers, 2 by 2
    """
    try:
        # 1. Run dataset
        response = get_dataset('9779')
        
        # 2. Get unique_code from each row and extract numbers
        excluded_numbers = []
        
        if response and isinstance(response, list):
            for row in response:
                if 'unique_code' in row and row['unique_code']:
                    unique_code = str(row['unique_code']).strip()
                    
                    # 3. Exclude first 4 letters (tote part)
                    if len(unique_code) > 4:
                        number_part = unique_code[4:]  # Remove first 4 letters
                        
                        # 4. Get the number (1-5000)
                        try:
                            tote_number = int(number_part)
                            if 1 <= tote_number <= 5000:
                                excluded_numbers.append(tote_number)
                        except ValueError:
                            # Skip if not a valid number
                            continue
        
        # Remove duplicates
        excluded_numbers = list(set(excluded_numbers))
        print(f"Found {len(excluded_numbers)} existing totes to exclude")
        
        # 6. Loop 1-5000 excluding those numbers
        available_numbers = []
        for i in range(1, 5001):
            if i not in excluded_numbers:
                available_numbers.append(i)
        
        print(f"Found {len(available_numbers)} available totes")
        
        # 7. Generate ZPL for remaining numbers, 2 by 2
        zpl_list = []
        for i in range(0, len(available_numbers), 2):
            tote1 = available_numbers[i]
            tote2 = available_numbers[i + 1] if i + 1 < len(available_numbers) else tote1
            
            zpl_code = generate_tote_pair_zpl(tote1, tote2)
            zpl_list.append(zpl_code)
        
        # Save ZPL codes to file
        with open('tote_labels.zpl', 'w', encoding='utf-8') as f:
            for zpl_code in zpl_list:
                f.write(zpl_code + "\n")
        
        print(f"Generated {len(zpl_list)} ZPL print jobs")
        print(f"Saved to tote_labels.zpl")
        
        return {
            "excluded_numbers": excluded_numbers,
            "available_numbers": available_numbers,
            "zpl_list": zpl_list,
            "total_zpl_jobs": len(zpl_list)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Test the simple function
    result = get_tote_livre()
    
    if "error" not in result:
        print(f"Excluded totes: {result['excluded_numbers'][:10]}...")  # Show first 10
        print(f"Available totes: {result['available_numbers'][:10]}...")  # Show first 10
        print(f"Total ZPL jobs: {result['total_zpl_jobs']}")
    else:
        print(f"Error: {result['error']}")

