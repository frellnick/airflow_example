"""
Partners

Define partner specific functions and inference operations.
TODO: Integrate into staging or profiler
"""
valid_partners = [
    'usbe', 
    'ushe',
    'ustc',
    'dws',
    'udoh',
    'adhoc',
]

def get_partner(filename:str) -> str:
    p = filename.split('_')[0].lower()
    if p in valid_partners:
        return p
    return 'adhoc'