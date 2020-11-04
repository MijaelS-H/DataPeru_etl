from etl.helpers import clean_tables

for table in ['itp_indicators_y_act_dept']:
    try:
        clean_tables(table)
    except:
        print('Table: {} does not exist'.format(table))