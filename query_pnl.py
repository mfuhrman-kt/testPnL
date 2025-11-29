#!/usr/bin/env python3
"""
Script to query PnL dashboard API and calculate aggregated PnL statistics.
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
        return response.json()
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
            # If kt_pnl_1_back is explicitly 0, we still want to use current_cumulative_pnl
            # Actually, wait - if it's 0.0, that's a valid value, not missing
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


def main():
    """Main function to run the script."""
    print("Querying PnL dashboard API...")
    data = query_pnl_data()
    
    if data is None:
        print("Failed to retrieve data from API.")
        return
    
    current_time = datetime.now()
    # Get timezone information
    try:
        # Try to get timezone-aware datetime with offset
        aware_time = datetime.now().astimezone()
        timezone_str = aware_time.strftime('%Z %z').strip()
        if not timezone_str:
            timezone_str = time.tzname[0] if time.daylight == 0 else time.tzname[1]
    except Exception:
        # Fallback to simple timezone name
        timezone_str = time.tzname[0] if time.daylight == 0 else time.tzname[1] if time.tzname else "UTC"
    
    print(f"Calculating statistics... [{current_time.strftime('%Y-%m-%d %H:%M:%S')} {timezone_str}]")
    stats = calculate_pnl_statistics(data)
    
    if stats:
        print_statistics(stats)
    else:
        print("Failed to calculate statistics.")


if __name__ == "__main__":
    main()

