import toml
import os

downloads_path = os.path.join(os.path.expanduser("~"), "Downloads", "secrets.toml")
output_file = downloads_path

with open("firestore-key.json") as json_file:
    json_text = json_file.read()

config = {"textkey": json_text}
toml_config = toml.dumps(config)

with open(output_file, "w") as target:
    target.write(toml_config)
