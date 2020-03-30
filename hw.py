import sys

print(sys.argv)

file_name = sys.argv[0]
from_text = sys.argv[1]
print(f'Hello {from_text} from {__name__}')
