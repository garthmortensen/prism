from __future__ import annotations

import base32_crockford as b32


def generate_human_id(seq_id: int, width: int = 3, prefix: str | None = None) -> str:
    """
    Generate a human-friendly ID from a sequential integer.
    
    Args:
        seq_id: The sequential integer ID.
        width: The minimum width of the resulting string (padded with zeros).
        prefix: Optional prefix to prepend to the ID.
        
    Returns:
        A Crockford Base32 encoded string with a checksum, optionally prefixed.
        Format: {prefix}{padded_base32}{checksum} (lowercase)
    """
    # 1. Get raw encoding with checksum (e.g., 1 -> "15")
    encoded_full = b32.encode(seq_id, checksum=True)
    
    # 2. Split payload and checksum (last char is checksum)
    payload = encoded_full[:-1]
    checksum = encoded_full[-1]
    
    # 3. Pad payload
    padded_payload = payload.zfill(width)
    
    # 4. Construct ID
    result = f"{padded_payload}{checksum}"
    
    if prefix:
        result = f"{prefix}{result}"
        
    return result.lower()

