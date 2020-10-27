def INDICATOR(df, geo_id):
    df['CONCYTEC_1'] = df['P213_aux_1']
    df['CONCYTEC_2'] = (df['P213_aux_1'] / df['count']) * 100
    df['CONCYTEC_3'] = df['P213_aux_2']
    df['CONCYTEC_4'] = (df['P213_aux_2'] / df['count']) * 100
    df['CONCYTEC_5'] = df['P213_aux_3']
    df['CONCYTEC_6'] = (df['P213_aux_3'] / df['count']) * 100
    df['CONCYTEC_7'] = df['P213_aux_4']
    df['CONCYTEC_8'] = (df['P213_aux_4'] / df['count']) * 100
    df['CONCYTEC_9'] = df['P214_aux_1']
    df['CONCYTEC_10'] = (df['P214_aux_1'] / df['count']) * 100
    df['CONCYTEC_11'] = df['P214_aux_2']
    df['CONCYTEC_12'] = (df['P214_aux_2'] / df['count']) * 100
    df['CONCYTEC_13'] = df['P214_aux_3']
    df['CONCYTEC_14'] = (df['P214_aux_3'] / df['count']) * 100
    df['CONCYTEC_15'] = df['P214_aux_4']
    df['CONCYTEC_16'] = (df['P214_aux_4'] / df['count']) * 100
    df['CONCYTEC_17'] = df['P214_aux_5']
    df['CONCYTEC_18'] = (df['P214_aux_5'] / df['count']) * 100
    df['CONCYTEC_19'] = df['P214_aux_6']
    df['CONCYTEC_20'] = (df['P214_aux_6'] / df['count']) * 100
    df['CONCYTEC_21'] = df['P214_aux_7']      #tiene nan que no se consideran en ninguna categoria 
    df['CONCYTEC_22'] = (df['P214_aux_7'] / df['count']) * 100
    df['CONCYTEC_23'] = df['P217']
    df['CONCYTEC_24'] = (df['P217'] / df['count']) * 100
    df['CONCYTEC_25'] = df['P404_1_1'] / df['count'] #promedio, tiene nan 
    df['CONCYTEC_26'] = df['P404_2_1'] / df['count'] #promedio, tiene nan 
    df['CONCYTEC_27'] = df['P328_1_1'] / df['count'] #promedio, tiene nan ... ? promedio --> es binario 
    df['CONCYTEC_28'] = df['P328_2_1'] / df['count'] #promedio, tiene nan ... ? ? promedio --> es binario 
    df['CONCYTEC_29'] = df['P328_1_1_aux_1'] / df['P213_aux_1']  # ???
    df['CONCYTEC_30'] = df['P328_2_1_aux_1'] / df['P213_aux_1']  # ???
    df['CONCYTEC_31'] = df['P328_1_1_aux_2'] / df['P213_aux_2']  # ???
    df['CONCYTEC_32'] = df['P328_2_1_aux_2'] / df['P213_aux_2'] # ??
    df['CONCYTEC_33'] = df['P328_1_1_aux_3'] / df['P213_aux_3'] # ??
    df['CONCYTEC_34'] = df['P328_2_1_aux_3'] / df['P213_aux_3'] # ??
    df['CONCYTEC_35'] = df['P328_1_1_aux_4'] / df['P213_aux_4'] # ??
    df['CONCYTEC_36'] = df['P328_2_1_aux_4'] / df['P213_aux_4'] # ??
    df['CONCYTEC_37'] = df['P328_1_2'] / df['count']
    df['CONCYTEC_38'] = df['P328_2_2'] / df['count']
    df['CONCYTEC_39'] = df['P328_1_2_aux_1'] / df['P213_aux_1']
    df['CONCYTEC_40'] = df['P328_2_2_aux_1'] / df['P213_aux_1']
    df['CONCYTEC_41'] = df['P328_1_2_aux_2'] / df['P213_aux_2']
    df['CONCYTEC_42'] = df['P328_2_2_aux_2'] / df['P213_aux_2']
    df['CONCYTEC_43'] = df['P328_1_2_aux_3'] / df['P213_aux_3']
    df['CONCYTEC_44'] = df['P328_2_2_aux_3'] / df['P213_aux_3']
    df['CONCYTEC_45'] = df['P328_1_2_aux_4'] / df['P213_aux_4']
    df['CONCYTEC_46'] = df['P328_2_2_aux_4'] / df['P213_aux_4']
    df['CONCYTEC_47'] = df['P404_1_1']
    df['CONCYTEC_48'] = (df['P404_1_1'] / df['P404_1_4']) * 100
    df['CONCYTEC_49'] = df['P404_2_1']
    df['CONCYTEC_50'] = (df['P404_2_1'] / df['P404_2_4']) * 100
    df['CONCYTEC_51'] = df['P404_1_2']
    df['CONCYTEC_52'] = (df['P404_1_2'] / df['P404_1_4']) * 100
    df['CONCYTEC_53'] = df['P404_2_2']
    df['CONCYTEC_54'] = (df['P404_2_2'] / df['P404_2_4']) * 100
    df['CONCYTEC_55'] = df['P404_1_3']
    df['CONCYTEC_56'] = (df['P404_1_3'] / df['P404_1_4']) * 100
    df['CONCYTEC_57'] = df['P404_2_3']
    df['CONCYTEC_58'] = (df['P404_2_3'] / df['P404_2_4']) * 100
    df['CONCYTEC_59'] = df['P329_1_1']
    df['CONCYTEC_60'] = (df['P329_1_1'] / df['P329_aux_1']) * 100
    df['CONCYTEC_61'] = df['P329_2_1']
    df['CONCYTEC_62'] = (df['P329_2_1'] / df['P329_aux_2']) * 100
    df['CONCYTEC_63'] = df['P329_1_2']
    df['CONCYTEC_64'] = (df['P329_1_2'] / df['P329_aux_1']) * 100
    df['CONCYTEC_65'] = df['P329_2_2']
    df['CONCYTEC_66'] = (df['P329_2_2'] / df['P329_aux_2']) * 100
    df['CONCYTEC_67'] = df['P329_1_3']
    df['CONCYTEC_68'] = (df['P329_1_3'] / df['P329_aux_1']) * 100
    df['CONCYTEC_69'] = df['P329_2_3']
    df['CONCYTEC_70'] = (df['P329_2_3'] / df['P329_aux_2']) * 100
    df['CONCYTEC_71'] = df['P329_1_4']
    df['CONCYTEC_72'] = (df['P329_1_4'] / df['P329_aux_1']) * 100
    df['CONCYTEC_73'] = df['P329_2_4']
    df['CONCYTEC_74'] = (df['P329_2_4'] / df['P329_aux_2']) * 100
    df['CONCYTEC_75'] = df['P329_1_5']
    df['CONCYTEC_76'] = (df['P329_1_5'] / df['P329_aux_1']) * 100
    df['CONCYTEC_77'] = df['P329_2_5']
    df['CONCYTEC_78'] = (df['P329_2_5'] / df['P329_aux_2']) * 100
    df['CONCYTEC_79'] = df['P329_1_6']
    df['CONCYTEC_80'] = (df['P329_1_6'] / df['P329_aux_1']) * 100
    df['CONCYTEC_81'] = df['P329_2_6']
    df['CONCYTEC_82'] = (df['P329_2_6'] / df['P329_aux_2']) * 100
    
    df = df[[geo_id, 'CONCYTEC_1', 'CONCYTEC_2', 'CONCYTEC_3', 'CONCYTEC_4', 'CONCYTEC_5', 'CONCYTEC_6', 'CONCYTEC_7', 'CONCYTEC_8', 
         'CONCYTEC_9', 'CONCYTEC_10', 'CONCYTEC_11', 'CONCYTEC_12', 'CONCYTEC_13', 'CONCYTEC_14', 'CONCYTEC_15', 'CONCYTEC_16', 'CONCYTEC_17', 
         'CONCYTEC_18', 'CONCYTEC_19', 'CONCYTEC_20', 'CONCYTEC_21', 'CONCYTEC_22', 'CONCYTEC_23', 'CONCYTEC_24', 'CONCYTEC_25', 'CONCYTEC_26', 
         'CONCYTEC_27', 'CONCYTEC_28', 'CONCYTEC_29', 'CONCYTEC_30', 'CONCYTEC_31', 'CONCYTEC_32', 'CONCYTEC_33', 'CONCYTEC_34', 'CONCYTEC_35', 
         'CONCYTEC_36', 'CONCYTEC_37', 'CONCYTEC_38', 'CONCYTEC_39', 'CONCYTEC_40', 'CONCYTEC_41', 'CONCYTEC_42', 'CONCYTEC_43', 'CONCYTEC_44', 
         'CONCYTEC_45', 'CONCYTEC_46', 'CONCYTEC_47', 'CONCYTEC_48', 'CONCYTEC_49', 'CONCYTEC_50', 'CONCYTEC_51', 'CONCYTEC_52', 'CONCYTEC_53', 
         'CONCYTEC_54', 'CONCYTEC_55', 'CONCYTEC_56', 'CONCYTEC_57', 'CONCYTEC_58', 'CONCYTEC_59', 'CONCYTEC_60', 'CONCYTEC_61', 'CONCYTEC_62', 
         'CONCYTEC_63', 'CONCYTEC_64', 'CONCYTEC_65', 'CONCYTEC_66', 'CONCYTEC_67', 'CONCYTEC_68', 'CONCYTEC_69', 'CONCYTEC_70', 'CONCYTEC_71', 
         'CONCYTEC_72', 'CONCYTEC_73', 'CONCYTEC_74', 'CONCYTEC_75', 'CONCYTEC_76', 'CONCYTEC_77', 'CONCYTEC_78', 'CONCYTEC_79', 'CONCYTEC_80', 
         'CONCYTEC_81', 'CONCYTEC_82']]
    
    return df