import json
import random
import numpy as np
import time
import argparse
from kafka import KafkaProducer
from datetime import datetime, timedelta
import uuid

def load_user_data(filename='user_data.json'):
    """Load user data from JSON file."""
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def random_point_in_radius(center_lat, center_lon, radius_km):
    """Generate a random GPS coordinate within radius_km of center."""
    radius_deg_lat = radius_km / 111.0
    radius_deg_lon = radius_km / (111.0 * np.cos(np.radians(center_lat)))
    
    angle = random.uniform(0, 2 * np.pi)
    distance = random.uniform(0, 1) ** 0.5
    
    lat = center_lat + distance * radius_deg_lat * np.sin(angle)
    lon = center_lon + distance * radius_deg_lon * np.cos(angle)
    
    return round(lat, 6), round(lon, 6)

def generate_transaction_time(base_date, is_daytime=True):
    """Generate a random transaction time, biased toward daytime."""
    if is_daytime:
        # Most transactions between 7am and 10pm
        hour = np.random.normal(14, 4)
        hour = max(0, min(23, int(hour)))
    else:
        hour = random.randint(0, 23)
    
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    return base_date.replace(hour=hour, minute=minute, second=second)

def get_travel_destinations():
    """Return a list of international travel destinations."""
    destinations = [
        (48.8566, 2.3522),    # Paris
        (51.5074, -0.1278),   # London
        (40.4168, -3.7038),   # Madrid
        (41.9028, 12.4964),   # Rome
        (52.5200, 13.4050),   # Berlin
        (48.2082, 16.3738),   # Vienna
        (59.3293, 18.0686),   # Stockholm
        (40.7128, -74.0060),  # New York
        (35.6762, 139.6503),  # Tokyo
        (1.3521, 103.8198),   # Singapore
        (25.2048, 55.2708),   # Dubai
        (-33.8688, 151.2093), # Sydney
        (55.7558, 37.6173),   # Moscow
        (37.7749, -122.4194), # San Francisco
        (34.0522, -118.2437), # Los Angeles
        (41.0082, 28.9784),   # Istanbul
        (50.0755, 14.4378),   # Prague
        (47.4979, 19.0402),   # Budapest
        (38.7223, -9.1393),   # Lisbon
        (60.1699, 24.9384),   # Helsinki
    ]
    return destinations

def calculate_flight_duration(lat1, lon1, lat2, lon2):
    """Estimate flight duration based on distance (rough approximation)."""
    # Haversine-like approximation for distance
    dlat = abs(lat2 - lat1)
    dlon = abs(lon2 - lon1)
    distance_deg = (dlat**2 + dlon**2)**0.5
    distance_km = distance_deg * 111  # rough approximation
    
    # Average flight speed ~800 km/h + 2 hours for boarding/landing
    flight_hours = distance_km / 800.0 + 2.0
    return max(3, min(flight_hours, 20))  # Between 3 and 20 hours

def get_scam_success_probability(scam_index, age):
    """Calculate probability of falling for a scam based on scam_index and age."""
    # Base probability from scam index
    # scam_index 0: ~75% chance of being scammed (low resilience)
    # scam_index 10: 0% chance of being scammed (high resilience)
    if scam_index >= 10:
        base_prob = 0.0
    elif scam_index == 0:
        base_prob = 0.75
    else:
        # Normal distribution-like mapping
        # Higher scam index = lower probability of being scammed
        base_prob = 0.75 * (1 - scam_index / 10.0)
    
    # Age multiplier: users around 30 are most resilient (multiplier ~0.7)
    # Very young (18) and very old (90) are more vulnerable (multiplier up to 1.5)
    age_factor = 1.0 + 0.5 * ((age - 30) / 30.0) ** 2
    age_factor = max(0.7, min(age_factor, 1.8))
    
    final_prob = base_prob * age_factor
    return min(final_prob, 0.95)  # Cap at 95%

def generate_transactions(user_data_file='user_data.json', simulation_days=30, output_file='transactions.json'):
    """Generate credit card transactions for all users over simulation_days."""
    
    data = load_user_data(user_data_file)
    users = data['users']
    
    # Build lookup structures
    user_dict = {}
    card_to_user = {}
    for user in users:
        user_dict[user['user_id']] = user
        for card in user['credit_cards']:
            card_to_user[card['card_id']] = user['user_id']
    
    # Travel state tracking
    travel_state = {}  # user_id -> {'traveling': bool, 'destination': (lat, lon), 'return_date': datetime, 'flight_end': datetime}
    
    # Scam state tracking
    active_scams = []  # list of active scam dictionaries
    
    all_transactions = []
    
    start_date = datetime(2024, 1, 1)
    destinations = get_travel_destinations()
    
    print(f"Simulating {simulation_days} days of transactions for {len(users)} users...")
    
    for day in range(simulation_days):
        current_date = start_date + timedelta(days=day)
        daily_card_spending = {}  # card_id -> amount spent today
        
        if day % 10 == 0:
            print(f"  Processing day {day + 1}/{simulation_days}...")
        
        # --- SCAM GENERATION FOR THE DAY ---
        # Each day, there's a chance for various scams to occur
        scam_types = ['spam', 'big', 'round_number', 'offshore', 'silent']
        
        for scam_type in scam_types:
            # Each scam type has a daily probability of occurring
            scam_probability = {
                'spam': 0.15,
                'big': 0.10,
                'round_number': 0.08,
                'offshore': 0.07,
                'silent': 0.05,
            }
            
            if random.random() < scam_probability[scam_type]:
                # Select 100 random users as targets
                targets = random.sample(users, min(100, len(users)))
                
                for target_user in targets:
                    success_prob = get_scam_success_probability(
                        target_user['scam_index'], target_user['age']
                    )
                    
                    if random.random() < success_prob:
                        # Scam succeeds on this user
                        # Pick a random card from the user
                        target_card = random.choice(target_user['credit_cards'])
                        
                        scam_entry = {
                            'type': scam_type,
                            'user_id': target_user['user_id'],
                            'card_id': target_card['card_id'],
                            'card_limit': target_card['limit'],
                            'start_day': day,
                            'home_lat': target_user['home_location']['latitude'],
                            'home_lon': target_user['home_location']['longitude'],
                        }
                        
                        if scam_type == 'silent':
                            # Silent scams persist for multiple days
                            scam_entry['duration_days'] = random.randint(7, 30)
                            scam_entry['amount'] = round(random.uniform(1, 10), 2)
                            scam_entry['time_hour'] = random.randint(1, 4)  # Night time
                            scam_entry['time_minute'] = random.randint(0, 59)
                        
                        active_scams.append(scam_entry)
        
        # --- PROCESS SCAM TRANSACTIONS ---
        scams_to_remove = []
        for idx, scam in enumerate(active_scams):
            card_id = scam['card_id']
            user_id = scam['user_id']
            card_limit = scam['card_limit']
            
            if card_id not in daily_card_spending:
                daily_card_spending[card_id] = 0
            
            if scam['type'] == 'spam':
                # Rapid small transactions - only on the day it starts
                if scam['start_day'] == day:
                    num_spam_transactions = random.randint(10, 50)
                    base_hour = random.randint(0, 20)
                    for i in range(num_spam_transactions):
                        amount = round(random.uniform(0.50, 15.00), 2)
                        spent_so_far = daily_card_spending.get(card_id, 0)
                        limit_left = max(0, card_limit - spent_so_far)
                        
                        if amount > limit_left:
                            break
                        
                        # Rapid transactions within a short time window
                        t_minute = base_hour * 60 + i * random.randint(1, 3)
                        t_hour = min(23, t_minute // 60)
                        t_min = t_minute % 60
                        
                        timestamp = current_date.replace(
                            hour=t_hour, minute=t_min, second=random.randint(0, 59)
                        )
                        
                        lat, lon = random_point_in_radius(
                            scam['home_lat'], scam['home_lon'], 50
                        )
                        
                        transaction = {
                            'card_id': card_id,
                            'user_id': user_id,
                            'gps': {'latitude': lat, 'longitude': lon},
                            'amount': amount,
                            'limit_left': round(limit_left - amount, 2),
                            'timestamp': timestamp.isoformat(),
                            'is_scam': True,
                            'scam_type': 'spam'
                        }
                        all_transactions.append(transaction)
                        daily_card_spending[card_id] = spent_so_far + amount
                    
                    scams_to_remove.append(idx)
            
            elif scam['type'] == 'big':
                # One big transaction
                if scam['start_day'] == day:
                    amount = round(random.uniform(500, 5000), 2)
                    spent_so_far = daily_card_spending.get(card_id, 0)
                    limit_left = max(0, card_limit - spent_so_far)
                    
                    if amount <= limit_left:
                        timestamp = generate_transaction_time(current_date, is_daytime=True)
                        lat, lon = random_point_in_radius(
                            scam['home_lat'], scam['home_lon'], 50
                        )
                        
                        transaction = {
                            'card_id': card_id,
                            'user_id': user_id,
                            'gps': {'latitude': lat, 'longitude': lon},
                            'amount': amount,
                            'limit_left': round(limit_left - amount, 2),
                            'timestamp': timestamp.isoformat(),
                            'is_scam': True,
                            'scam_type': 'big'
                        }
                        all_transactions.append(transaction)
                        daily_card_spending[card_id] = spent_so_far + amount
                    
                    scams_to_remove.append(idx)
            
            elif scam['type'] == 'round_number':
                # Normal-looking transactions but all round numbers
                if scam['start_day'] == day:
                    num_transactions = random.randint(3, 8)
                    round_amount = random.choice([10, 20, 25, 50, 100, 150, 200, 250])
                    
                    for i in range(num_transactions):
                        spent_so_far = daily_card_spending.get(card_id, 0)
                        limit_left = max(0, card_limit - spent_so_far)
                        
                        if round_amount > limit_left:
                            break
                        
                        timestamp = generate_transaction_time(current_date, is_daytime=True)
                        lat, lon = random_point_in_radius(
                            scam['home_lat'], scam['home_lon'], 50
                        )
                        
                        transaction = {
                            'card_id': card_id,
                            'user_id': user_id,
                            'gps': {'latitude': lat, 'longitude': lon},
                            'amount': float(round_amount),
                            'limit_left': round(limit_left - round_amount, 2),
                            'timestamp': timestamp.isoformat(),
                            'is_scam': True,
                            'scam_type': 'round_number'
                        }
                        all_transactions.append(transaction)
                        daily_card_spending[card_id] = spent_so_far + round_amount
                    
                    scams_to_remove.append(idx)
            
            elif scam['type'] == 'offshore':
                # Normal transactions from a faraway location
                if scam['start_day'] == day:
                    # Pick a far-away destination
                    offshore_dest = random.choice(destinations)
                    num_transactions = random.randint(2, 6)
                    
                    for i in range(num_transactions):
                        amount = round(max(5, np.random.normal(250, 100)), 2)
                        spent_so_far = daily_card_spending.get(card_id, 0)
                        limit_left = max(0, card_limit - spent_so_far)
                        
                        if amount > limit_left:
                            break
                        
                        timestamp = generate_transaction_time(current_date, is_daytime=True)
                        lat, lon = random_point_in_radius(
                            offshore_dest[0], offshore_dest[1], 30
                        )
                        
                        transaction = {
                            'card_id': card_id,
                            'user_id': user_id,
                            'gps': {'latitude': lat, 'longitude': lon},
                            'amount': amount,
                            'limit_left': round(limit_left - amount, 2),
                            'timestamp': timestamp.isoformat(),
                            'is_scam': True,
                            'scam_type': 'offshore'
                        }
                        all_transactions.append(transaction)
                        daily_card_spending[card_id] = spent_so_far + amount
                    
                    scams_to_remove.append(idx)
            
            elif scam['type'] == 'silent':
                # Small amount every night at the same time
                days_active = day - scam['start_day']
                if 0 <= days_active < scam['duration_days']:
                    amount = scam['amount']
                    spent_so_far = daily_card_spending.get(card_id, 0)
                    limit_left = max(0, card_limit - spent_so_far)
                    
                    if amount <= limit_left:
                        timestamp = current_date.replace(
                            hour=scam['time_hour'],
                            minute=scam['time_minute'],
                            second=random.randint(0, 5)  # Very consistent
                        )
                        lat, lon = random_point_in_radius(
                            scam['home_lat'], scam['home_lon'], 10
                        )
                        
                        transaction = {
                            'card_id': card_id,
                            'user_id': user_id,
                            'gps': {'latitude': lat, 'longitude': lon},
                            'amount': amount,
                            'limit_left': round(limit_left - amount, 2),
                            'timestamp': timestamp.isoformat(),
                            'is_scam': True,
                            'scam_type': 'silent'
                        }
                        all_transactions.append(transaction)
                        daily_card_spending[card_id] = spent_so_far + amount
                
                elif days_active >= scam['duration_days']:
                    scams_to_remove.append(idx)
        
        # Remove completed scams (in reverse order to preserve indices)
        for idx in sorted(set(scams_to_remove), reverse=True):
            if idx < len(active_scams):
                active_scams.pop(idx)
        
        # --- NORMAL USER TRANSACTIONS ---
        for user in users:
            user_id = user['user_id']
            spend_index = user['spend_index']
            travel_index = user['travel_index']
            home_lat = user['home_location']['latitude']
            home_lon = user['home_location']['longitude']
            
            # Determine if user is traveling
            if user_id not in travel_state:
                travel_state[user_id] = {
                    'traveling': False,
                    'destination': None,
                    'return_date': None,
                    'flight_end': None,
                    'flight_back_start': None,
                }
            
            state = travel_state[user_id]
            
            # Check if user returns from travel
            if state['traveling'] and state['return_date'] and current_date >= state['return_date']:
                # Check if still in return flight
                if state.get('flight_back_start') and current_date < state['flight_back_start'] + timedelta(hours=state.get('flight_duration', 5)):
                    # Still in flight back, no transactions
                    continue
                else:
                    state['traveling'] = False
                    state['destination'] = None
                    state['return_date'] = None
                    state['flight_end'] = None
                    state['flight_back_start'] = None
            
            # Check if user starts traveling today
            if not state['traveling']:
                # Travel probability per day based on travel_index
                # Index 0: never travels
                # Index 1: ~1 week per year -> ~1.9% daily chance of starting a trip
                # Index 2: ~3 weeks per year -> ~5.7% daily chance
                # Index 3: ~6 weeks per year -> ~11.5% daily chance
                # Index 4: always traveling
                
                travel_daily_prob = {
                    0: 0.0,
                    1: 0.005,   # ~1-2 trips per year, short
                    2: 0.016,   # ~3 weeks away per year
                    3: 0.035,   # ~6 weeks away per year
                    4: 1.0,     # always traveling
                }
                
                if random.random() < travel_daily_prob[travel_index]:
                    # Start a trip
                    dest = random.choice(destinations)
                    flight_duration = calculate_flight_duration(home_lat, home_lon, dest[0], dest[1])
                    
                    # Trip duration
                    if travel_index == 4:
                        trip_days = random.randint(3, 14)
                    elif travel_index == 3:
                        trip_days = random.randint(5, 14)
                    elif travel_index == 2:
                        trip_days = random.randint(4, 10)
                    else:
                        trip_days = random.randint(3, 7)
                    
                    state['traveling'] = True
                    state['destination'] = dest
                    state['flight_end'] = current_date + timedelta(hours=flight_duration)
                    state['return_date'] = current_date + timedelta(days=trip_days)
                    state['flight_duration'] = flight_duration
                    state['flight_back_start'] = state['return_date'] - timedelta(hours=flight_duration)
            
            # Determine current location
            if state['traveling']:
                # Check if still in outbound flight
                if state['flight_end'] and current_date < state['flight_end']:
                    # In flight, no transactions
                    continue
                # Check if in return flight
                if state.get('flight_back_start') and current_date >= state['flight_back_start']:
                    # In return flight, no transactions
                    continue
                
                current_lat = state['destination'][0]
                current_lon = state['destination'][1]
                transaction_radius = 30  # 30km radius at destination
            else:
                current_lat = home_lat
                current_lon = home_lon
                transaction_radius = 50  # 50km radius from home
            
            # Number of daily transactions based on spend index
            # Higher spend index = more transactions
            # Base: 1-3 transactions, modified by spend_index
            avg_transactions = 1 + spend_index * 0.5
            num_transactions = max(0, int(np.random.poisson(avg_transactions)))
            
            # Generate transactions for each card
            for card in user['credit_cards']:
                card_id = card['card_id']
                card_limit = card['limit']
                
                if card_id not in daily_card_spending:
                    daily_card_spending[card_id] = 0
                
                for _ in range(num_transactions):
                    # Transaction amount from normal distribution
                    amount = round(max(1, np.random.normal(250, 100)), 2)
                    
                    # Adjust amount by spend index (higher index = slightly higher amounts)
                    amount *= (0.5 + spend_index * 0.1)
                    amount = round(max(1, amount), 2)
                    
                    spent_so_far = daily_card_spending.get(card_id, 0)
                    limit_left = max(0, card_limit - spent_so_far)
                    
                    if amount > limit_left:
                        # Can't exceed limit
                        if limit_left > 1:
                            amount = round(min(amount, limit_left), 2)
                        else:
                            break
                    
                    # Generate GPS for transaction
                    lat, lon = random_point_in_radius(current_lat, current_lon, transaction_radius)
                    
                    # Generate timestamp (mostly daytime)
                    timestamp = generate_transaction_time(current_date, is_daytime=True)
                    
                    transaction = {
                        'card_id': card_id,
                        'user_id': user_id,
                        'gps': {'latitude': lat, 'longitude': lon},
                        'amount': amount,
                        'limit_left': round(limit_left - amount, 2),
                        'timestamp': timestamp.isoformat(),
                        'is_scam': False,
                        'scam_type': None
                    }
                    all_transactions.append(transaction)
                    daily_card_spending[card_id] = spent_so_far + amount
    
    # Sort all transactions by timestamp
    all_transactions.sort(key=lambda x: x['timestamp'])
    
    # Save to file
    output = {
        'simulation_days': simulation_days,
        'start_date': start_date.isoformat(),
        'end_date': (start_date + timedelta(days=simulation_days - 1)).isoformat(),
        'total_transactions': len(all_transactions),
        'total_scam_transactions': sum(1 for t in all_transactions if t['is_scam']),
        'transactions': all_transactions
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nTransaction generation complete!")
    print(f"Total transactions: {len(all_transactions)}")
    print(f"Total scam transactions: {output['total_scam_transactions']}")
    print(f"Scam percentage: {output['total_scam_transactions'] / len(all_transactions) * 100:.2f}%")
    print(f"Saved to {output_file}")
    
    # Print scam breakdown
    scam_counts = {}
    for t in all_transactions:
        if t['is_scam']:
            st = t['scam_type']
            scam_counts[st] = scam_counts.get(st, 0) + 1
    
    print("\nScam breakdown:")
    for scam_type, count in sorted(scam_counts.items()):
        print(f"  {scam_type}: {count} transactions")
    return all_transactions

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':
    random.seed(123)
    np.random.seed(123)
    
    # Parse execution arguments
    parser = argparse.ArgumentParser(description="Generate and process simulated transactions.")
    parser.add_argument('--write_to_file', action='store_true', help='Write the output directly to a JSON file. If omitted, streams sequentially to Kafka.')
    args = parser.parse_args()

    # Generate the transactions array
    transactions = generate_transactions(
        user_data_file='user_data.json',
        simulation_days=30,
        output_file='transactions.json'
    )

    # If --write_to_file is NOT passed, stream them sequentially to Kafka
    if not args.write_to_file:
        print("\nConnecting to Kafka broker and initializing stream...")
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=serializer
        )
        
        print("Posting transactions sequentially by timestamp to 'transactions' topic...")
        for tx in transactions:
            print(f"Producing new Transaction @ {datetime.now()} | Transaction Data = {tx}")
            producer.send('transactions', tx)
            time.sleep(0.01)
            
        producer.flush()
        producer.close()
        print("\nAll transactions streamed successfully.")