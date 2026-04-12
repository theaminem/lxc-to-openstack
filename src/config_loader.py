import yaml
import sys


def load_config(path="config.yml"):
    try:
        with open(path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"[ERREUR] Fichier de configuration introuvable : {path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"[ERREUR] Fichier de configuration mal forme : {e}")
        sys.exit(1)
