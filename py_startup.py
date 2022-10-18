import traceback
from extract.extract_channel import ext_channels



try:
    ext_channels()

except:
    traceback.print_exc()
finally:
    pass