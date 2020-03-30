# Create Spark Context (using SparkSession)
# Read the data
# Apply Schema
# Process (multiple iterations)
# Write the data

import config
import sys

from pyspark.sql.functions import sum


def get_spark_session(app_name, exec_mode):
    from pyspark.sql import SparkSession
    spark = SparkSession. \
        builder. \
        appName(app_name). \
        master(exec_mode). \
        getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    return spark


def read(spark, input_base_dir, table, schema):
    file_name = f'{input_base_dir}/{table}/part-00000'
    df = spark.read.csv(file_name,
                        sep=',',
                        schema=schema
                        )
    return df


def get_groupby_agg_results(df, group_by_using, aggregate_using, agg_funcs):
    agg_results = df. \
        groupBy(group_by_using). \
        agg({aggregate_using: agg_funcs})
    return agg_results


def write(df, output_base_dir, output_dir, env, file_format):
    if env == 'prod':
        import getpass
        username = getpass.getuser()
        native_output_base_dir = output_base_dir.format(username=username)
    else:
        native_output_base_dir = output_base_dir
    output_path = f'{native_output_base_dir}/{output_dir}'
    df.write. \
        mode('overwrite'). \
        format(file_format). \
        save(output_path)
    return


order_items_schema = '''order_item_id INT, order_item_order_id INT, 
order_item_product_id INT, order_item_quantity INT, 
order_item_subtotal FLOAT, order_item_product_price FLOAT'''

env = sys.argv[1]
props = config.file_properties[env]

spark = get_spark_session('Revenue Per Order', props['exec_mode'])

order_items = read(spark, props['input_base_dir'], 'order_items', order_items_schema)

revenue_per_order = get_groupby_agg_results(order_items,
                                            'order_item_order_id',
                                            'order_item_subtotal',
                                            'sum'
                                            )
write(revenue_per_order, props['output_base_dir'], 'revenue_per_order', env, 'json')
