#!/usr/bin/env python3
"""
AWS Lambda function to query PnL dashboard API and calculate aggregated PnL statistics.
"""

import requests
import json
from collections import defaultdict
from datetime import datetime
import time

# API configuration
API_URL = "https://dashboard-ec2.ktinternal.com/api/pnl-dashboard/get-live-market-pnl-cache"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJtZnVocm1hbkBrYWxzaGl0cmFkaW5nLmNvbSIsImV4cCI6MTc2NTE0NTAyOSwiaWF0IjoxNzYyNTUzMDI5fQ.RbiMuGd8W1SPJvetP0wDkh-dW4usDLAv8mU_AB9fd38"
}


def query_pnl_data():
    """Query the PnL dashboard API and return the response."""
    try:
        response = requests.get(API_URL, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        # Print full response structure/details
        print("=" * 80)
        print("FULL RESPONSE DETAILS:")
        print("=" * 80)
        print(f"Response type: {type(data)}")
        print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        
        # Print all top-level keys and their types/values
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "results":
                    print(f"\n{key}: (array with {len(value) if isinstance(value, list) else 'unknown'} items)")
                else:
                    print(f"{key}: {value} (type: {type(value).__name__})")
        
        # Print response as JSON for full visibility
        print("\n" + "=" * 80)
        print("FULL RESPONSE JSON:")
        print("=" * 80)
        print(json.dumps(data, indent=2, default=str))
        print("=" * 80)
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error querying API: {e}")
        return None


def calculate_pnl_statistics(data):
    """Calculate total, category, and desk PnL statistics."""
    if not data or "results" not in data:
        print("Invalid data format")
        return None
    
    results = data["results"]
    
    # Initialize aggregators
    total_pnl = 0.0
    category_pnl = defaultdict(float)
    desk_pnl = defaultdict(float)
    
    # Process each result
    for result in results:
        kt_pnl_1_back = result.get("kt_pnl_1_back")
        if kt_pnl_1_back is None:
            kt_pnl_1_back = result.get("current_cumulative_pnl", 0.0)
        elif kt_pnl_1_back == 0.0:
            pass
        
        # Convert None to 0.0 if it's still None
        if kt_pnl_1_back is None:
            kt_pnl_1_back = 0.0
        
        # Add to total
        total_pnl += kt_pnl_1_back
        
        # Add to category total
        category = result.get("category", "Unknown")
        category_pnl[category] += kt_pnl_1_back
        
        # Add to desk total
        desk = result.get("desk", "Unknown")
        desk_pnl[desk] += kt_pnl_1_back
    
    return {
        "total": total_pnl,
        "by_category": dict(category_pnl),
        "by_desk": dict(desk_pnl)
    }


def print_statistics(stats):
    """Print the PnL statistics in a formatted way."""
    if not stats:
        return
    
    print(f"Total PnL: {stats['total']:.2f}\n")
    
    print("PnL by Category:")
    print("-" * 40)
    for category, pnl in sorted(stats['by_category'].items()):
        print(f"{category}: {pnl:.2f}")
    
    print("\nPnL by Desk:")
    print("-" * 40)
    for desk, pnl in sorted(stats['by_desk'].items()):
        print(f"{desk}: {pnl:.2f}")


def get_timezone_string():
    """Get timezone string for logging."""
    try:
        aware_time = datetime.now().astimezone()
        timezone_str = aware_time.strftime('%Z %z').strip()
        if not timezone_str:
            timezone_str = time.tzname[0] if time.daylight == 0 else time.tzname[1]
        return timezone_str
    except Exception:
        return time.tzname[0] if time.daylight == 0 else time.tzname[1] if time.tzname else "UTC"


def format_response(stats, timestamp, timezone_str):
    """Format statistics as a structured response."""
    if not stats:
        return None
    
    response = {
        "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        "timezone": timezone_str,
        "total_pnl": round(stats['total'], 2),
        "by_category": {cat: round(pnl, 2) for cat, pnl in sorted(stats['by_category'].items())},
        "by_desk": {desk: round(pnl, 2) for desk, pnl in sorted(stats['by_desk'].items())}
    }
    return response


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Args:
        event: Lambda event object (not used in this function)
        context: Lambda context object
        
    Returns:
        dict: API Gateway response with status code and body
    """
    try:
        print("Querying PnL dashboard API...")
        data = query_pnl_data()
        
        if data is None:
            error_message = "Failed to retrieve data from API."
            print(error_message)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': error_message,
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        current_time = datetime.now()
        timezone_str = get_timezone_string()
        
        print(f"Calculating statistics... [{current_time.strftime('%Y-%m-%d %H:%M:%S')} {timezone_str}]")
        stats = calculate_pnl_statistics(data)
        
        if stats:
            # Print statistics to CloudWatch logs
            print_statistics(stats)
            
            # Format and return JSON response
            response_body = format_response(stats, current_time, timezone_str)
            
            return {
                'statusCode': 200,
                'body': json.dumps(response_body, indent=2),
                'headers': {
                    'Content-Type': 'application/json'
                }
            }
        else:
            error_message = "Failed to calculate statistics."
            print(error_message)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': error_message,
                    'timestamp': datetime.now().isoformat()
                })
            }
            
    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            })
        }


def main():
    """Main function to run the script locally (for testing)."""
    print("Querying PnL dashboard API...")
    data = query_pnl_data()
    
    if data is None:
        print("Failed to retrieve data from API.")
        return
    
    current_time = datetime.now()
    timezone_str = get_timezone_string()
    
    print(f"Calculating statistics... [{current_time.strftime('%Y-%m-%d %H:%M:%S')} {timezone_str}]")
    stats = calculate_pnl_statistics(data)
    
    if stats:
        print_statistics(stats)
    else:
        print("Failed to calculate statistics.")


if __name__ == "__main__":
    main()

