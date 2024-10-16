import json

with open('./results.json', 'r') as file:
    data = json.load(file)

with open('./results.txt', 'r') as file:
    tp_results = file.readlines()

print("Checking q1...")

tp_q1 = ""
for line in tp_results:
    if line.startswith("[QUERY 1 - FINAL]:"):
        tp_q1 = line.split("[QUERY 1 - FINAL]: ")[1].strip()
        break

if tp_q1 == "Windows: " +str(data['q1']['windows'])+", Mac: " +str(data['q1']['mac'])+", Linux: " +str(data['q1']['linux']):
    print("q1 is correct")
else:
    print("Expected: ", data['q1'])
    print("Found: ", tp_q1)
    print("q1 is incorrect")


print("\nChecking q2...")

ok = True
for i, top_game in enumerate(data['q2']):
    found = False
    for line in tp_results:
        if line.startswith("[QUERY 2]: Top Game "+str(i+1)+": "+top_game['Name'] + " ("+str(top_game['Average playtime forever'])+")"):
            found = True
            break
    if not found:
        print("Expected: ", top_game['Name'])
        ok = False

if ok:
    print("q2 is correct")
else:
    print("q2 is incorrect")

print("\nChecking q3...")

ok = True
for i, top_game in enumerate(data['q3']):
    found = False
    for line in tp_results:
        if line.startswith("[QUERY 3]: Top Game "+str(i+1)+": "+top_game['Name'] + " ("+str(top_game['positive_score'])+")"):
            found = True
            break
    if not found:
        print("Expected: ", top_game['Name'])
        ok = False

if ok:
    print("q3 is correct")
else:
    print("q3 is incorrect")

print("\nChecking q4...")

ok = True
for i, top_game in enumerate(data['q4']):
    found = False
    for line in tp_results:
        if line.startswith("[QUERY 4 - PARCIAL]: "+top_game['Name']):
            found = True
            break
    if not found:
        print("Expected: ", top_game['Name'])
        ok = False

total_games = 0
for line in tp_results:
    if line.startswith("[QUERY 4 - PARCIAL]:"):
        total_games += 1

if total_games != len(data['q4']):
    print("Expected total games: ", len(data['q4']))
    print("Found total games: ", total_games)
    ok = False

if ok:
    print("q4 is correct")
else:
    print("q4 is incorrect")


print("\nChecking q5...")

ok = True
for i, top_game in enumerate(data['q5']):
    found = False
    for line in tp_results:
        if line.startswith("[QUERY 5 - PARCIAL]: "+top_game['Name'] + " ("+str(top_game['count'])+")"):
            found = True
            break
    if not found:
        print(f"Expected: {top_game['Name']} with {top_game["count"]} negative reviews")
        ok = False

total_games = 0
for line in tp_results:
    if line.startswith("[QUERY 5 - PARCIAL]:"):
        total_games += 1

if total_games != len(data['q5']):
    print("Expected total games: ", len(data['q5']))
    print("Found total games: ", total_games)
    ok = False

if ok:
    print("q5 is correct")
else:
    print("q5 is incorrect")
