# Create encoded file

import os
import json
import base64

def do_encode():
    event_path = 'events'
    event_key = 'event.json'
    output_key = 'event_enc.json'

    json_encoder_body(event_path, event_key, output_key)

def json_encoder_body(event_path, event_key, output_key):
    with open(os.path.join(event_path, event_key)) as event:
        print(event,"eventt")
        json_data = json.loads(event)

    encoded_body = base64.b64encode(json_data['Records'][0]['body'].encode('utf-8'))
    json_data['Records'][0]['body'] = str(encoded_body)

    with open(os.path.join(event_path, output_key), "w") as json_file:
        json.dump(json_data, json_file)


do_encode()