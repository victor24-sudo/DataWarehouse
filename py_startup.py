import traceback
from extract.extract_channel import ext_channels
from extract.extract_sales import ext_sales
from extract.extract_countries import ext_countries
from extract.extrac_customers import ext_customers
from extract.extract_products import ext_products
from extract.extrac_promotions import ext_promotions
from extract.extract_times import ext_times



try:
    ext_channels()
    ext_sales()
    ext_countries()
    ext_customers()
    ext_products()
    ext_promotions()
    ext_times()


except:
    traceback.print_exc()
finally:
    pass
