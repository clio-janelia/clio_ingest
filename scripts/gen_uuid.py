import uuid
import sys

# python gen_uuid <name>
uuid_str = str(uuid.uuid4())
print(f"{sys.argv[1]}_{uuid_str[-12:]}")
