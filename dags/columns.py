from dataclasses import dataclass
from typing import List

@dataclass
class TableMeta():
    schema: str
    table_namename: str
    columns: List[str]


STG_RESALE_PRICES_META = TableMeta('staging','stg_resale_prices',[
        'id',
        'transaction_month',
        'town',
        'flat_type',
        'block_num',
        'street_name',
        'storey_range',
        'floor_area_sqm',
        'flat_model',
        'lease_commence_date',
        'remaining_lease',
        'resale_price',
    ])

INT_RESALE_PRICES_META = TableMeta('warehouse', 'int_resale_prices', STG_RESALE_PRICES_META.columns + ['latitude','longitude'])

TABLE_META = {
    'stg_resale_prices': STG_RESALE_PRICES_META,
    'int_resale_prices': INT_RESALE_PRICES_META
}