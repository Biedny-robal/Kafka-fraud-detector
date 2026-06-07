import json
import random
import numpy as np
import uuid

def generate_user_data():
    """Generate user data until 10000 credit card IDs have been created."""
    
    # City centers (latitude, longitude)
    cities = {
        'Warszawa': (52.2297, 21.0122),
        'Kraków': (50.0647, 19.9450),
        'Wrocław': (51.1079, 17.0385),
        'Łódź': (51.7592, 19.4560),
        'Poznań': (52.4064, 16.9252),
    }
    
    # City probabilities
    city_probs = {
        'Warszawa': 0.10,
        'Kraków': 0.08,
        'Wrocław': 0.06,
        'Łódź': 0.06,
        'Poznań': 0.05,
        'rest': 0.65,
    }
    
    # Poland bounding box (approximate)
    poland_bounds = {
        'lat_min': 49.0,
        'lat_max': 54.8,
        'lon_min': 14.1,
        'lon_max': 24.15,
    }
    
    # Polish age distribution (approximate, ages 18-90)
    # Based on Polish demographics - larger working-age population, smaller elderly
    def generate_polish_age():
        """Generate age based on approximate Polish demographic distribution."""
        # Use a mixture to approximate Polish demographics
        # Poland has a bulge around 30-45 (born 1975-1990) and another around 55-65
        while True:
            r = random.random()
            if r < 0.3:
                # Young adults 18-30
                age = int(np.random.triangular(18, 25, 31))
            elif r < 0.65:
                # Middle aged 31-50
                age = int(np.random.triangular(31, 40, 51))
            elif r < 0.85:
                # Older working 51-65
                age = int(np.random.triangular(51, 58, 66))
            else:
                # Elderly 66-90
                age = int(np.random.triangular(66, 72, 91))
            
            if 18 <= age <= 90:
                return age
    
    def random_point_in_radius(center_lat, center_lon, radius_km):
        """Generate a random GPS coordinate within radius_km of center."""
        # Convert radius to degrees (approximate)
        radius_deg_lat = radius_km / 111.0
        radius_deg_lon = radius_km / (111.0 * np.cos(np.radians(center_lat)))
        
        # Random angle and distance
        angle = random.uniform(0, 2 * np.pi)
        distance = random.uniform(0, 1) ** 0.5  # sqrt for uniform distribution in circle
        
        lat = center_lat + distance * radius_deg_lat * np.sin(angle)
        lon = center_lon + distance * radius_deg_lon * np.cos(angle)
        
        return round(lat, 6), round(lon, 6)
    
    def generate_home_location():
        """Generate home GPS coordinates based on city distribution."""
        r = random.random()
        cumulative = 0
        
        for city, prob in city_probs.items():
            cumulative += prob
            if r <= cumulative:
                if city == 'rest':
                    # Random location in Poland (excluding major city centers to avoid overlap)
                    lat = random.uniform(poland_bounds['lat_min'], poland_bounds['lat_max'])
                    lon = random.uniform(poland_bounds['lon_min'], poland_bounds['lon_max'])
                    return round(lat, 6), round(lon, 6)
                else:
                    center_lat, center_lon = cities[city]
                    return random_point_in_radius(center_lat, center_lon, 30)
        
        # Fallback to rest of Poland
        lat = random.uniform(poland_bounds['lat_min'], poland_bounds['lat_max'])
        lon = random.uniform(poland_bounds['lon_min'], poland_bounds['lon_max'])
        return round(lat, 6), round(lon, 6)
    
    def generate_travel_index():
        """Generate travel index with specified distribution."""
        r = random.random()
        if r < 0.05:
            return 0
        elif r < 0.15:
            return 1
        elif r < 0.95:
            return 2
        elif r < 0.99:
            return 3
        else:
            return 4
    
    def generate_scam_index():
        """Generate scam index 0-10 following normal distribution."""
        while True:
            value = np.random.normal(5, 2)
            value = round(value)
            if 0 <= value <= 10:
                return int(value)
    
    def generate_spend_index():
        """Generate spend index 0-10 following normal distribution."""
        while True:
            value = np.random.normal(5, 2)
            value = round(value)
            if 0 <= value <= 10:
                return int(value)
    
    def generate_credit_cards():
        """Generate credit card count based on distribution."""
        r = random.random()
        if r < 0.95:
            count = 1
        elif r < 0.99:
            count = 2
        else:
            count = random.randint(3, 8)
        
        cards = []
        for _ in range(count):
            card_id = str(uuid.uuid4()).replace('-', '')[:16].upper()
            limit = random.randrange(1000, 10500, 500)  # 1000 to 10000 in increments of 500
            cards.append({
                'card_id': card_id,
                'limit': limit
            })
        
        return cards
    
    # Generate users until we have 10000 credit card IDs
    users = []
    total_cards = 0
    user_id_counter = 1
    all_card_ids = set()
    
    while total_cards < 10000:
        user_id = f"USER_{user_id_counter:06d}"
        user_id_counter += 1
        
        age = generate_polish_age()
        home_lat, home_lon = generate_home_location()
        travel_index = generate_travel_index()
        scam_index = generate_scam_index()
        spend_index = generate_spend_index()
        cards = generate_credit_cards()
        
        # Ensure unique card IDs
        unique_cards = []
        for card in cards:
            while card['card_id'] in all_card_ids:
                card['card_id'] = str(uuid.uuid4()).replace('-', '')[:16].upper()
            all_card_ids.add(card['card_id'])
            unique_cards.append(card)
        
        user = {
            'user_id': user_id,
            'age': age,
            'home_location': {
                'latitude': home_lat,
                'longitude': home_lon
            },
            'travel_index': travel_index,
            'scam_index': scam_index,
            'spend_index': spend_index,
            'credit_cards': unique_cards
        }
        
        users.append(user)
        total_cards += len(unique_cards)
    
    # Save to JSON
    output = {
        'total_users': len(users),
        'total_credit_cards': total_cards,
        'users': users
    }
    
    with open('user_data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"Generated {len(users)} users with {total_cards} credit cards.")
    print(f"Saved to user_data.json")

if __name__ == '__main__':
    random.seed(42)
    np.random.seed(42)
    generate_user_data()