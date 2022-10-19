import traceback
from extract.extract_channel import ext_channels
from extract.extract_sales import ext_sales



try:
    # ext_channels()
    ext_sales()

except:
    traceback.print_exc()
finally:
    pass