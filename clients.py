import psycopg2
import requests
import time
import os
from dotenv import load_dotenv
from typing import Optional, Dict, List

# Load environment variables from .env file
load_dotenv()

# Debug: Print what was loaded (remove after testing)
print(f"🔍 Debug Info:")
print(f"   PG_HOST: {os.getenv('PG_HOST')}")
print(f"   GOOGLE_API_KEY: {'Found' if os.getenv('GOOGLE_API_KEY') else 'NOT FOUND'}")
print()

# --- Configuration loaded from .env ---

# Database configuration
PG_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'database': os.getenv('PG_DATABASE')
}

# Google Geocoding API key
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GEOCODING_URL = 'https://maps.googleapis.com/maps/api/geocode/json'

# ----------------------------------------


def get_clients_without_geolocation(conn) -> List[Dict]:
    """Fetch clients that don't have geolocation data"""
    query = """
        WITH ct AS (
            SELECT * FROM geolocation g WHERE type = 'client'
        ),
        main AS (
            SELECT c.* FROM ct LEFT JOIN public.clients c ON c.id = ct.id
        )
        SELECT c.id, c.client_name, c.address, c.pincode, c.state, c.city
        FROM clients c
        WHERE c.client_name IS NOT NULL 
        AND c.id NOT IN (SELECT id FROM main WHERE id IS NOT NULL)
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        return [dict(zip(columns, row)) for row in results]


def build_address_string(client: Dict) -> str:
    """Build a complete address string from client data"""
    address_parts = []
    
    if client.get('address'):
        address_parts.append(client['address'])
    if client.get('city'):
        address_parts.append(client['city'])
    if client.get('state'):
        address_parts.append(client['state'])
    if client.get('pincode'):
        address_parts.append(str(client['pincode']))
    
    address_parts.append('India')
    
    return ', '.join(address_parts)


def get_geolocation(address: str) -> Optional[Dict]:
    """Fetch geolocation data from Google Geocoding API"""
    try:
        params = {
            'address': address,
            'key': GOOGLE_API_KEY
        }
        
        response = requests.get(GEOCODING_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            formatted_address = data['results'][0]['formatted_address']
            
            return {
                'latitude': location['lat'],
                'longitude': location['lng'],
                'formatted_address': formatted_address,
                'place_id': data['results'][0].get('place_id', '')
            }
        else:
            return {
                'error': data.get('status', 'UNKNOWN'),
                'message': data.get('error_message', 'No results found')
            }
            
    except requests.exceptions.RequestException as e:
        return {
            'error': 'API_REQUEST_FAILED',
            'message': str(e)
        }
    except Exception as e:
        return {
            'error': 'UNEXPECTED_ERROR',
            'message': str(e)
        }


def save_geolocation(conn, client_id: int, geo_data: Dict) -> bool:
    """Save geolocation data to the database only if it doesn't exist"""
    try:
        # First check if the client already exists in geolocation table
        check_query = """
            SELECT id FROM geolocation WHERE id = %s AND type = 'client'
        """
        
        with conn.cursor() as cur:
            cur.execute(check_query, (client_id,))
            existing = cur.fetchone()
            
            if existing:
                print(f"  ⚠️  Client ID {client_id} already exists in geolocation table. Skipping insert.")
                return False
            
            # Insert new geolocation record
            insert_query = """
                INSERT INTO geolocation (id, latitude, longitude, type)
                VALUES (%s, %s, %s, 'client')
            """
            
            cur.execute(insert_query, (
                client_id,
                geo_data['latitude'],
                geo_data['longitude']
            ))
            conn.commit()
            print(f"  ✅ Inserted into geolocation table")
            return True
        
    except Exception as e:
        print(f"  ❌ Database error: {e}")
        conn.rollback()
        return False


def process_clients(limit: int = None):
    """Main function to fetch and save geolocation for clients
    
    Args:
        limit: Number of clients to process (default: None for all clients)
    """
    # Check if necessary config values are loaded
    if not all([PG_CONFIG['host'], GOOGLE_API_KEY]):
        print("❌ Configuration Error: Database host or API key not loaded. Check your .env file.")
        return

    conn = None
    results = []
    
    try:
        # Connect to database
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(**PG_CONFIG)
        print("✅ Connected successfully\n")
        
        # Fetch clients without geolocation
        print("📋 Fetching clients without geolocation...")
        all_clients = get_clients_without_geolocation(conn)
        print(f"✅ Found {len(all_clients)} total clients")
        
        # Apply limit
        if limit:
            clients = all_clients[:limit]
            print(f"🎯 Processing first {len(clients)} clients (limit applied)\n")
        else:
            clients = all_clients
            print(f"🎯 Processing ALL {len(clients)} clients\n")
        
        print("=" * 80)
        
        if not clients:
            print("ℹ️  No clients to process. Exiting.")
            return
        
        # Process each client
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for idx, client in enumerate(clients, 1):
            client_id = client['id']
            client_name = client['client_name']
            address = build_address_string(client)
            
            print(f"\n[{idx}/{len(clients)}] Client ID: {client_id}")
            print(f"{'─' * 80}")
            print(f"👤 Client Name: {client_name}")
            print(f"📍 Address: {address}")
            
            # Get geolocation
            geo_data = get_geolocation(address)
            
            if geo_data and 'latitude' in geo_data:
                print(f"✅ Geocoding SUCCESS")
                print(f"   Latitude:  {geo_data['latitude']}")
                print(f"   Longitude: {geo_data['longitude']}")
                print(f"   Formatted: {geo_data['formatted_address']}")
                
                # Save to database
                saved = save_geolocation(conn, client_id, geo_data)
                
                if saved:
                    success_count += 1
                    results.append({
                        'client_id': client_id,
                        'client_name': client_name,
                        'status': 'success',
                        'latitude': geo_data['latitude'],
                        'longitude': geo_data['longitude'],
                        'formatted_address': geo_data['formatted_address']
                    })
                else:
                    skipped_count += 1
                    results.append({
                        'client_id': client_id,
                        'client_name': client_name,
                        'status': 'skipped',
                        'reason': 'Already exists in database'
                    })
            else:
                print(f"❌ Geocoding FAILED")
                print(f"   Error: {geo_data.get('error', 'Unknown')}")
                print(f"   Message: {geo_data.get('message', 'N/A')}")
                failed_count += 1
                
                results.append({
                    'client_id': client_id,
                    'client_name': client_name,
                    'status': 'failed',
                    'error': geo_data.get('error', 'Unknown'),
                    'message': geo_data.get('message', 'N/A')
                })
            
            # Rate limiting: Google API has limits, add delay between requests
            if idx < len(clients):
                time.sleep(0.2)  # 200ms delay between requests
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 SUMMARY")
        print("=" * 80)
        print(f"Total clients processed: {len(clients)}")
        print(f"✅ Successfully saved: {success_count}")
        print(f"⏭️  Skipped (already exists): {skipped_count}")
        print(f"❌ Failed: {failed_count}")
        print("=" * 80)
        
        return results
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")


if __name__ == "__main__":
    # Process ALL clients
    print("🚀 Starting Client Geocoding Script (Processing ALL clients)")
    print("=" * 80)
    results = process_clients()
    
    # To process limited number, change to:
    # results = process_clients(limit=2)