import traceback
from extract.extract_channel import ext_channels
from extract.extract_sales import ext_sales
from extract.extract_countries import ext_countries
from extract.extrac_customers import ext_customers



try:
    # ext_channels()
    # ext_sales()
    # ext_countries()
    ext_customers()


except:
    traceback.print_exc()
finally:
    pass