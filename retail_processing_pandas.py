# Read the data
# Apply Schema
# Process (multiple iterations)
# Write the data

import pandas as pd


def read(input_base_dir, table, schema):
    file_name = f'{input_base_dir}/{table}/part-00000'
    df = pd.read_csv(file_name,
                     sep=',',
                     header=None,
                     names=schema
                     )
    return df


def get_groupby_agg_results(df, group_by_using, aggregate_using, agg_funcs):
    agg_results = df. \
        groupby(group_by_using)[aggregate_using]. \
        agg(agg_funcs)
    return agg_results


def write(df, output_base_dir, output_file, env):
    if env == 'prod':
        import getpass
        username = getpass.getuser()
        native_output_base_dir = output_base_dir.format(username=username)
    else:
        native_output_base_dir = output_base_dir
    output_file_name = f'{native_output_base_dir}/{output_file}'
    df.to_csv(output_file_name)
    return


order_items_schema = [
    "order_item_id",
    "order_item_order_id",
    "order_item_product_id",
    "order_item_quantity",
    "order_item_subtotal",
    "order_item_product_price"
]


from config import file_properties
import sys

env = sys.argv[1]
props = file_properties[env]

order_items = read(props['input_base_dir'], 'order_items', order_items_schema)
revenue_per_order = get_groupby_agg_results(order_items,
                                            'order_item_order_id',
                                            'order_item_subtotal',
                                            ['sum', 'count']
                                            )
write(revenue_per_order, props['output_base_dir'], 'revenue_per_order.csv', env)