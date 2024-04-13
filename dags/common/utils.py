import math

def calc_dist(coord1, coord2):
    # Convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [coord1[1], coord1[0], coord2[1], coord2[0]])
    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def snake_case_to_normal_case(snake_str):
    words = snake_str.split('_')
    words[0] = words[0].capitalize()  # Capitalize the first letter
    return ' '.join(words)