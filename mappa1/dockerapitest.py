import docker

try:
    client = docker.from_env()
    print(client.ping())
except Exception as e:
    print(f"Error: {e}")
