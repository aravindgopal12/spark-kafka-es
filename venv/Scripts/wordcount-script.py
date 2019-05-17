#!D:\python_programs\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'wordcount==1.0','console_scripts','wordcount'
__requires__ = 'wordcount==1.0'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('wordcount==1.0', 'console_scripts', 'wordcount')()
    )
