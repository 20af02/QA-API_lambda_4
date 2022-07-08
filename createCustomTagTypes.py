import json


with open('ES.json', 'r', encoding='utf-8') as f:
    rows = json.loads("[" +
                      f.read().replace("}\n{", "},\n{") +
                      "]")


finalMessage = {"enumerationValues": []}
attribue_format = {"value": ""}


for row in rows:
    try:
        tags = row['tags']
        tags = tags[0].split(',')
        for tag in tags:
            if tag != '':
                attribue_format["value"] = tag
                if attribue_format not in finalMessage["enumerationValues"]:
                    finalMessage["enumerationValues"].append(
                        attribue_format.copy())
    except:
        continue


with open("tagTypes.json", "w") as outfile:
    json.dump(finalMessage, outfile)
