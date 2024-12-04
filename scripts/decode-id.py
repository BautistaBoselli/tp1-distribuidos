def decode_number(number):
    # Convert the number to a 64-bit binary string
    binary_str = f"{number:064b}"
    
    # Extract the parts based on the bit positions
    clientId_high = int(binary_str[0:8], 2)
    clientId_low = int(binary_str[8:16], 2)
    queryId = int(binary_str[16:24], 2)
    shardId = int(binary_str[24:32], 2)
    appId = int(binary_str[32:], 2)
    
    return {
        'clientId': (clientId_high << 8) | clientId_low,
        'queryId': queryId,
        'shardId': shardId,
        'appId': appId
    }

# Your number
number = 282039026176260138

result = decode_number(number)
print(f"clientId (2 bytes): {result['clientId']}")
print(f"queryId (1 byte): {result['queryId']}")
print(f"shardId (1 byte): {result['shardId']}")
print(f"appId (4 bytes): {result['appId']}")