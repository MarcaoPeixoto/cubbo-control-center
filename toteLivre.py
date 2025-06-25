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


def generate_filtered_tote_labels_list(start_tote=1, end_tote=5000, excluded_totes=None):
    """
    Generate a list of ZPL codes for pairs of tote labels, excluding existing totes
    Each ZPL code prints 2 labels: tote1&tote2, tote3&tote4, etc.
    """
    if excluded_totes is None:
        excluded_totes = set()
    
    zpl_list = []
    available_totes = []
    
    # Collect all available totes (not in excluded list)
    for tote_num in range(start_tote, end_tote + 1):
        if tote_num not in excluded_totes:
            available_totes.append(tote_num)
    
    # Generate pairs of available totes
    for i in range(0, len(available_totes), 2):
        tote1 = available_totes[i]
        tote2 = available_totes[i + 1] if i + 1 < len(available_totes) else tote1  # If odd number, duplicate last
        
        zpl_code = generate_tote_pair_zpl(tote1, tote2)
        zpl_list.append(zpl_code)
    
    return zpl_list, available_totes


def get_tote_livre():
    try:
        # Call the Metabase query to get existing totes
        response = get_dataset('9779')
        
        # Extract unique_code column to get existing tote numbers
        excluded_totes = set()
        if response and isinstance(response, list) and len(response) > 0:
            for row in response:
                if 'unique_code' in row and row['unique_code']:
                    unique_code = str(row['unique_code']).strip()
                    # Extract number from strings like "tote1", "tote2", etc.
                    if unique_code.lower().startswith('tote'):
                        try:
                            # Remove "tote" prefix and convert to integer
                            tote_num = int(unique_code[4:])  # Remove "tote" (4 characters)
                            excluded_totes.add(tote_num)
                        except (ValueError, TypeError):
                            # Skip if it's not a valid number after "tote"
                            continue
        
        print(f"Found {len(excluded_totes)} existing totes to exclude")
        
        # Generate ZPL list for available totes (1 to 5000, excluding existing ones)
        zpl_list, available_totes = generate_filtered_tote_labels_list(1, 5000, excluded_totes)
        
        # Save all ZPL codes to a file, one per line
        with open('tote_labels.zpl', 'w', encoding='utf-8') as f:
            for zpl_code in zpl_list:
                f.write(zpl_code + "\n")
        
        print(f"Generated {len(zpl_list)} ZPL print jobs for {len(available_totes)} available totes (2 labels per job)")
        print(f"Excluded {len(excluded_totes)} existing totes")
        print(f"Saved to tote_labels.zpl")
        
        # Return both the dataset response and ZPL information
        return {
            "dataset": response,
            "zpl_data": {
                "zpl_list": zpl_list,
                "available_totes": available_totes,
                "excluded_totes": list(excluded_totes),
                "total_zpl_jobs": len(zpl_list),
                "total_available_totes": len(available_totes),
                "total_excluded_totes": len(excluded_totes)
            }
        }
    except Exception as e:
        print(f"Error getting tote livre data: {str(e)}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Test the function
    data = get_tote_livre()
    print(f"Found {len(data)} total records in dataset")
    
    # You can also generate labels for a specific range
    # For example, totes 1-10:
    sample_zpl_list, sample_totes = generate_filtered_tote_labels_list(1, 10, {2, 4, 6})  # Exclude 2, 4, 6
    print(f"Sample: Generated {len(sample_zpl_list)} ZPL jobs for available totes 1-10 (excluding 2,4,6)")
    print(f"Available totes: {sample_totes}")

