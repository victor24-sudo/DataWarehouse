import pandas


def get_surrogate_keys(table_name, natural_key_cols, db_context):
    natural_keys_str = ','.join(natural_key_cols)
    return pandas.read_sql_query(f'SELECT ID_surr, {natural_keys_str} FROM {table_name}', db_context).set_index(natural_key_cols).to_dict()['ID_surr']


def merge(table_name, natural_key_cols, dataframe, db_context):
    existing_table_key_pairs = get_surrogate_keys(table_name=table_name, natural_key_cols=natural_key_cols, db_context=db_context)
    columns = dataframe.columns.tolist()
    if len(columns) > 0:
        for natural_key in natural_key_cols:
            columns.remove(natural_key)

    if len(natural_key_cols) == 1:
        dataframe['ID_surr'] = dataframe.apply(lambda row: existing_table_key_pairs.get(*tuple(row[natural_key_cols].values), None), axis=1)
    else:
        dataframe['ID_surr'] = dataframe.apply(lambda row: existing_table_key_pairs.get(tuple(row[natural_key_cols].values), None), axis=1)

    elements_to_update = dataframe[dataframe['ID_surr'].notnull()]

    if not elements_to_update.empty:
        existing_elements = pandas.read_sql_query('SELECT * FROM {table_name} WHERE ID_surr IN ({ids})'.format(table_name=table_name, ids=','.join(elements_to_update['ID_surr'].astype(str))), db_context)
        elements_to_update = elements_to_update.merge(existing_elements, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        update_query = f'UPDATE {table_name} SET {",".join([f"{col} = %s" for col in columns])} WHERE ID_surr = %s'
        for index, row in elements_to_update.iterrows():
            db_context.execute(update_query, tuple(row[columns].values) + (row['ID_surr'],))

    elements_to_insert = dataframe[dataframe['ID_surr'].isnull()]
    if not elements_to_insert.empty:
        elements_to_insert.to_sql(table_name, db_context, if_exists='append', index=False)