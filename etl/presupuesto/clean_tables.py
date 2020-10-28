
from etl.helpers import clean_tables

for table in ['mef_presupuesto_gastos', 'mef_presupuesto_ingresos']:
    try:
        clean_tables(table)
    except:
        print('Table: {} does not exist'.format(table))