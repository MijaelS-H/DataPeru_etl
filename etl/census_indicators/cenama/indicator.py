def INDICATOR_GEO(df, geo_id):
    df['CENAMA_1'] = df['p36_1']
    df['CENAMA_2'] = df['p36_2']
    df['CENAMA_3'] = df['p39_1'] + df['p39_2'] + df['p39_3'] + df['p39_4'] + df['p39_5'] + df['p39_6']
    df['CENAMA_4'] = (df['CENAMA_2'] / df['p36_2']) * 100
    df['CENAMA_5'] = df['p36_3']
    df['CENAMA_6'] = df['p37']
    df['CENAMA_7'] = df['p38']
    df['CENAMA_9'] = df['p47_1'] 
    df['CENAMA_10'] = (df['p47_1'] / df['count']) * 100
    df['CENAMA_12'] = df['p47_2']
    df['CENAMA_13'] = (df['p47_2'] / df['count']) * 100
    df['CENAMA_15'] = df['p47_3']
    df['CENAMA_16'] = (df['p47_3'] / df['count']) * 100
    df['CENAMA_18'] = df['p55_1']
    df['CENAMA_19'] = (df['p55_1'] / df['count']) * 100
    df['CENAMA_21'] = df['p55_2']
    df['CENAMA_22'] = (df['p55_2'] / df['count']) * 100
    df['CENAMA_24'] = df['p55_3']
    df['CENAMA_25'] = (df['p55_3'] / df['count']) * 100
    df['CENAMA_27'] = df['p55_4']
    df['CENAMA_28'] = (df['p55_4'] / df['count']) * 100
    df['CENAMA_30'] = df['p49_7']
    df['CENAMA_31'] = (df['p49_7'] / df['count']) * 100
    df['CENAMA_33'] = df['p49_10']
    df['CENAMA_34'] = (df['p49_10'] / df['count']) * 100
    df['CENAMA_36'] = df['p49_13']
    df['CENAMA_37'] = (df['p49_13'] / df['count']) * 100
    df['CENAMA_39'] = df['p49_8']
    df['CENAMA_40'] = (df['p49_8'] / df['count']) * 100
    df['CENAMA_44'] = df['p49_9']
    df['CENAMA_45'] = (df['p49_9'] / df['count']) * 100
    df['CENAMA_47'] = df['p56_1e_total'] + df['p56_2e_total'] + df['p56_3e_total'] + df['p56_4e_total'] + df['p56_5e_total'] + df['p56_6e_total'] + df['p56_7e_total']
    df['CENAMA_48'] = df['p56_1e_total'] 
    df['CENAMA_49'] = (df['p56_1e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_50'] = df['p56_2e_total'] 
    df['CENAMA_51'] = (df['p56_2e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_52'] = df['p56_3e_total'] 
    df['CENAMA_53'] = (df['p56_3e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_54'] = df['p56_4e_total'] 
    df['CENAMA_55'] = (df['p56_4e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_56'] = df['p56_5e_total'] 
    df['CENAMA_57'] = (df['p56_5e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_58'] = df['p56_6e_total'] 
    df['CENAMA_59'] = (df['p56_6e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_60'] = df['p56a_1b'] + df['p56a_1d'] + df['p56a_2b'] + df['p56a_2d'] + df['p56a_3b'] + df['p56a_3d'] + df['p56a_4b'] + df['p56a_4d'] + df['p56a_5b'] + df['p56a_5d'] + df['p56a_6b'] + df['p56a_6d'] + df['p56a_7b'] + df['p56a_7d']
    df['CENAMA_61'] = (df['CENAMA_60'] / df['CENAMA_47']) * 100
    df['CENAMA_62'] = df['p56a_1a'] + df['p56a_1c'] + df['p56a_2a'] + df['p56a_2c'] + df['p56a_3a'] + df['p56a_3c'] + df['p56a_4a'] + df['p56a_4c'] + df['p56a_5a'] + df['p56a_5c'] + df['p56a_6a'] + df['p56a_6c'] + df['p56a_7a'] + df['p56a_7c']
    df['CENAMA_63'] = (df['CENAMA_62'] / df['CENAMA_47']) * 100
    df['CENAMA_64'] = df['p56a_1a'] + df['p56a_1b'] + df['p56a_2a'] + df['p56a_2b'] + df['p56a_3a'] + df['p56a_3b'] + df['p56a_4a'] + df['p56a_4b'] + df['p56a_5a'] + df['p56a_5b'] + df['p56a_6a'] + df['p56a_6b'] + df['p56a_7a'] + df['p56a_7b']
    df['CENAMA_65'] = (df['CENAMA_64'] / df['CENAMA_47']) * 100
    df['CENAMA_66'] = df['p56a_1d'] + df['p56a_1c'] + df['p56a_2d'] + df['p56a_2c'] + df['p56a_3d'] + df['p56a_3c'] + df['p56a_4d'] + df['p56a_4c'] + df['p56a_5d'] + df['p56a_5c'] + df['p56a_6d'] + df['p56a_6c'] + df['p56a_7d'] + df['p56a_7c']
    df['CENAMA_67'] = (df['CENAMA_66'] / df['CENAMA_47']) * 100
    df['CENAMA_69'] = df['p61_1']
    df['CENAMA_70'] = (df['p61_1'] / df['count']) * 100
    df['CENAMA_72'] = df['p61a1_4'] / df['count']
    df['CENAMA_74'] = df['p61_2']
    df['CENAMA_75'] = (df['p61_2'] / df['count']) * 100
    df['CENAMA_77'] = df['p61a2_4'] / df['count']
    df['CENAMA_79'] = df['p61_3']
    df['CENAMA_80'] = (df['p61_3'] / df['count']) * 100
    df['CENAMA_82'] = df['p61a3_4'] / df['count']
    df['CENAMA_84'] = df['p62']
    df['CENAMA_85'] = (df['p62'] / df['count']) * 100
    df['CENAMA_87'] = df['p63_1']
    df['CENAMA_88'] = (df['p63_1'] / df['count']) * 100
    df['CENAMA_90'] = df['p63_2']
    df['CENAMA_91'] = (df['p63_2'] / df['count']) * 100
    df['CENAMA_93'] = df['p63_3']
    df['CENAMA_94'] = (df['p63_3'] / df['count']) * 100
    df['CENAMA_96'] = df['p63_4']
    df['CENAMA_97'] = (df['p63_4'] / df['count']) * 100
    df['CENAMA_99'] = df['p63_5']
    df['CENAMA_100'] = (df['p63_5'] / df['count']) * 100
    df['CENAMA_102'] = df['p63_6']
    df['CENAMA_103'] = (df['p63_6'] / df['count']) * 100
    df['CENAMA_105'] = df['p63_8']
    df['CENAMA_106'] = (df['p63_8'] / df['count']) * 100
    df['CENAMA_108'] = df['p64_7a_total'] / df['count']
    df['CENAMA_111'] = df['p64_1a'] / df['count']
    df['CENAMA_114'] = df['p64_2a'] / df['count']
    df['CENAMA_117'] = df['p64_3a'] / df['count']
    df['CENAMA_120'] = df['p64_4a'] / df['count']
    df['CENAMA_123'] = df['p64_5a'] / df['count']
    df['CENAMA_125'] = df['p64_5b_total'] / df['count']
    df['CENAMA_128'] = df['p64_1b'] / df['count']
    df['CENAMA_131'] = df['p64_2b'] / df['count']
    df['CENAMA_134'] = df['p64_3b'] / df['count']
    df['CENAMA_136'] = df['p40_2']
    df['CENAMA_139'] = df['p30_1']
    df['CENAMA_140'] = (df['p30_1'] / df['count']) * 100
    df['CENAMA_142'] = df['p30_2']
    df['CENAMA_143'] = (df['p30_2'] / df['count']) * 100
    df['CENAMA_145'] = df['p30_3']
    df['CENAMA_146'] = (df['p30_3'] / df['count']) * 100
    df['CENAMA_148'] = df['p31_1']
    df['CENAMA_149'] = (df['p31_1'] / df['count']) * 100
    df['CENAMA_151'] = df['p31_2']
    df['CENAMA_152'] = (df['p31_2'] / df['count']) * 100
    df['CENAMA_154'] = df['p34a']
    df['CENAMA_155'] = (df['p34a'] / df['count']) * 100
    df['CENAMA_157'] = df['formality_0']
    df['CENAMA_158'] = (df['formality_0'] / df['count']) * 100
    df['CENAMA_159'] = df['formality_1']
    df['CENAMA_160'] = (df['formality_1'] / df['count']) * 100
    df['CENAMA_161'] = df['formality_2']
    df['CENAMA_162'] = (df['formality_2'] / df['count']) * 100
    df['CENAMA_163'] = df['formality_3']
    df['CENAMA_164'] = (df['formality_3'] / df['count']) * 100
    df['CENAMA_166'] = df['p30_4']
    df['CENAMA_167'] = (df['p30_4'] / df['count']) * 100
    df['CENAMA_169'] = df['p49_5']
    df['CENAMA_170'] = (df['p49_5'] / df['count']) * 100
    df['CENAMA_171'] = df['p50']
    df['CENAMA_174'] = df['p54_1']
    df['CENAMA_175'] = (df['p54_1'] / df['count']) * 100
    df['CENAMA_177'] = df['p54_2']
    df['CENAMA_178'] = (df['p54_2'] / df['count']) * 100
    df['CENAMA_180'] = df['p54_3']
    df['CENAMA_181'] = (df['p54_3'] / df['count']) * 100
    df['CENAMA_183'] = df['p59a_1']
    df['CENAMA_184'] = (df['p59a_1'] / df['count']) * 100
    df['CENAMA_186'] = df['p59a_2']
    df['CENAMA_187'] = (df['p59a_2'] / df['count']) * 100
    df['CENAMA_189'] = df['p59a_3']
    df['CENAMA_190'] = (df['p59a_3'] / df['count']) * 100
    df['CENAMA_192'] = df['p59a_4']
    df['CENAMA_193'] = (df['p59a_4'] / df['count']) * 100
    df['CENAMA_195'] = df['p59a_5']
    df['CENAMA_196'] = (df['p59a_5'] / df['count']) * 100
    df['CENAMA_198'] = df['p59a_6']
    df['CENAMA_199'] = (df['p59a_6'] / df['count']) * 100
    df['CENAMA_201'] = df['p59a_7']
    df['CENAMA_202'] = (df['p59a_7'] / df['count']) * 100
    df['CENAMA_204'] = df['p59a_9']
    df['CENAMA_205'] = (df['p59a_9'] / df['count']) * 100
    df['CENAMA_207'] = df['p60_1']
    df['CENAMA_208'] = (df['p60_1'] / df['count']) * 100
    df['CENAMA_210'] = df['p60_2']
    df['CENAMA_211'] = (df['p60_2'] / df['count']) * 100
    df['CENAMA_213'] = df['p60_3']
    df['CENAMA_214'] = (df['p60_3'] / df['count']) * 100
    df['CENAMA_216'] = df['p60_4']
    df['CENAMA_217'] = (df['p60_4'] / df['count']) * 100
    df['CENAMA_219'] = df['p60_5']
    df['CENAMA_220'] = (df['p60_5'] / df['count']) * 100
    df['CENAMA_222'] = df['p60_6']
    df['CENAMA_223'] = (df['p60_6'] / df['count']) * 100
    
    df = df[[geo_id, 'CENAMA_1', 'CENAMA_2', 'CENAMA_3', 'CENAMA_4', 'CENAMA_5', 'CENAMA_6', 'CENAMA_7', 'CENAMA_9', 'CENAMA_10',
         'CENAMA_12', 'CENAMA_13', 'CENAMA_15', 'CENAMA_16', 'CENAMA_18', 'CENAMA_19', 'CENAMA_21', 'CENAMA_22', 'CENAMA_24', 'CENAMA_25',
         'CENAMA_27', 'CENAMA_28', 'CENAMA_30', 'CENAMA_31', 'CENAMA_33', 'CENAMA_34', 'CENAMA_36', 'CENAMA_37', 'CENAMA_39', 'CENAMA_40', 
         'CENAMA_44', 'CENAMA_45', 'CENAMA_47', 'CENAMA_48', 'CENAMA_49', 'CENAMA_50', 'CENAMA_51', 'CENAMA_52', 'CENAMA_53', 'CENAMA_54',
         'CENAMA_55', 'CENAMA_56', 'CENAMA_57', 'CENAMA_58', 'CENAMA_59', 'CENAMA_60', 'CENAMA_61', 'CENAMA_62', 'CENAMA_63', 'CENAMA_64', 
         'CENAMA_65', 'CENAMA_66', 'CENAMA_67', 'CENAMA_69', 'CENAMA_70', 'CENAMA_72', 'CENAMA_74', 'CENAMA_75', 'CENAMA_77', 'CENAMA_79',
         'CENAMA_80', 'CENAMA_82', 'CENAMA_84', 'CENAMA_85', 'CENAMA_87', 'CENAMA_88', 'CENAMA_90', 'CENAMA_91', 'CENAMA_93', 'CENAMA_94',
         'CENAMA_96', 'CENAMA_97', 'CENAMA_99', 'CENAMA_100', 'CENAMA_102', 'CENAMA_103', 'CENAMA_105', 'CENAMA_106', 'CENAMA_108', 'CENAMA_111',
         'CENAMA_114', 'CENAMA_117', 'CENAMA_120', 'CENAMA_123', 'CENAMA_125', 'CENAMA_128', 'CENAMA_131', 'CENAMA_134', 'CENAMA_136', 'CENAMA_139',
         'CENAMA_140', 'CENAMA_142', 'CENAMA_143', 'CENAMA_145', 'CENAMA_146', 'CENAMA_148', 'CENAMA_149', 'CENAMA_151', 'CENAMA_152', 'CENAMA_154',
         'CENAMA_155', 'CENAMA_157', 'CENAMA_158', 'CENAMA_159', 'CENAMA_160', 'CENAMA_161', 'CENAMA_162', 'CENAMA_163', 'CENAMA_164', 'CENAMA_166',
         'CENAMA_167', 'CENAMA_169', 'CENAMA_170', 'CENAMA_171', 'CENAMA_174', 'CENAMA_175', 'CENAMA_177', 'CENAMA_178', 'CENAMA_180', 'CENAMA_181', 
         'CENAMA_183', 'CENAMA_184', 'CENAMA_186', 'CENAMA_187', 'CENAMA_189', 'CENAMA_190', 'CENAMA_192', 'CENAMA_193', 'CENAMA_195', 'CENAMA_196', 
         'CENAMA_198', 'CENAMA_199', 'CENAMA_201', 'CENAMA_202', 'CENAMA_204', 'CENAMA_205', 'CENAMA_207', 'CENAMA_208', 'CENAMA_210', 'CENAMA_211', 
         'CENAMA_213', 'CENAMA_214', 'CENAMA_216', 'CENAMA_217', 'CENAMA_219', 'CENAMA_220', 'CENAMA_222', 'CENAMA_223']]
    
    return df

def INDICATOR_MARKET(df, market_id):
    df['CENAMA_1'] = df['p36_1']
    df['CENAMA_2'] = df['p36_2']
    df['CENAMA_3'] = df['p39_1'] + df['p39_2'] + df['p39_3'] + df['p39_4'] + df['p39_5'] + df['p39_6']
    df['CENAMA_5'] = df['p36_3']
    df['CENAMA_6'] = df['p37']
    df['CENAMA_7'] = df['p38']
    df['CENAMA_8'] = df['p47_1']
    df['CENAMA_11'] = df['p47_2']
    df['CENAMA_14'] = df['p47_3']
    df['CENAMA_17'] = df['p55_1']
    df['CENAMA_20'] = df['p55_2']
    df['CENAMA_23'] = df['p55_3']
    df['CENAMA_26'] = df['p55_4']
    df['CENAMA_29'] = df['p49_7']
    df['CENAMA_32'] = df['p49_10']
    df['CENAMA_35'] = df['p49_13']
    df['CENAMA_38'] = df['p49_8']
    df['CENAMA_41'] = df['p49b_1']
    df['CENAMA_42'] = df['p49c_1']
    df['CENAMA_43'] = df['p49_9']
    df['CENAMA_46'] = df['p49c_2']
    df['CENAMA_47'] = df['p56_1e_total'] + df['p56_2e_total'] + df['p56_3e_total'] + df['p56_4e_total'] + df['p56_5e_total'] + df['p56_6e_total'] + df['p56_7e_total']
    df['CENAMA_48'] = df['p56_1e_total'] 
    df['CENAMA_49'] = (df['p56_1e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_50'] = df['p56_2e_total'] 
    df['CENAMA_51'] = (df['p56_2e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_52'] = df['p56_3e_total'] 
    df['CENAMA_53'] = (df['p56_3e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_54'] = df['p56_4e_total'] 
    df['CENAMA_55'] = (df['p56_4e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_56'] = df['p56_5e_total'] 
    df['CENAMA_57'] = (df['p56_5e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_58'] = df['p56_6e_total'] 
    df['CENAMA_59'] = (df['p56_6e_total'] / df['CENAMA_47']) * 100
    df['CENAMA_60'] = df['p56a_1b'] + df['p56a_1d'] + df['p56a_2b'] + df['p56a_2d'] + df['p56a_3b'] + df['p56a_3d'] + df['p56a_4b'] + df['p56a_4d'] + df['p56a_5b'] + df['p56a_5d'] + df['p56a_6b'] + df['p56a_6d'] + df['p56a_7b'] + df['p56a_7d']
    df['CENAMA_61'] = (df['CENAMA_60'] / df['CENAMA_47']) * 100
    df['CENAMA_62'] = df['p56a_1a'] + df['p56a_1c'] + df['p56a_2a'] + df['p56a_2c'] + df['p56a_3a'] + df['p56a_3c'] + df['p56a_4a'] + df['p56a_4c'] + df['p56a_5a'] + df['p56a_5c'] + df['p56a_6a'] + df['p56a_6c'] + df['p56a_7a'] + df['p56a_7c']
    df['CENAMA_63'] = (df['CENAMA_62'] / df['CENAMA_47']) * 100
    df['CENAMA_64'] = df['p56a_1a'] + df['p56a_1b'] + df['p56a_2a'] + df['p56a_2b'] + df['p56a_3a'] + df['p56a_3b'] + df['p56a_4a'] + df['p56a_4b'] + df['p56a_5a'] + df['p56a_5b'] + df['p56a_6a'] + df['p56a_6b'] + df['p56a_7a'] + df['p56a_7b']
    df['CENAMA_65'] = (df['CENAMA_64'] / df['CENAMA_47']) * 100
    df['CENAMA_66'] = df['p56a_1d'] + df['p56a_1c'] + df['p56a_2d'] + df['p56a_2c'] + df['p56a_3d'] + df['p56a_3c'] + df['p56a_4d'] + df['p56a_4c'] + df['p56a_5d'] + df['p56a_5c'] + df['p56a_6d'] + df['p56a_6c'] + df['p56a_7d'] + df['p56a_7c']
    df['CENAMA_67'] = (df['CENAMA_66'] / df['CENAMA_47']) * 100
    df['CENAMA_68'] = df['p61_1']
    df['CENAMA_71'] = df['p61a1_4']
    df['CENAMA_73'] = df['p61_2']
    df['CENAMA_76'] = df['p61a2_4']
    df['CENAMA_78'] = df['p61_3']
    df['CENAMA_81'] = df['p61a3_4']
    df['CENAMA_83'] = df['p62']
    df['CENAMA_86'] = df['p63_1']
    df['CENAMA_89'] = df['p63_2']
    df['CENAMA_92'] = df['p63_3']
    df['CENAMA_95'] = df['p63_4']
    df['CENAMA_98'] = df['p63_5']
    df['CENAMA_101'] = df['p63_6']
    df['CENAMA_104'] = df['p63_8']
    df['CENAMA_107'] = df['p64_7a_total']
    df['CENAMA_109'] = df['p64_1a']
    df['CENAMA_110'] = (df['p64_1a'] / df['p64_7a_total']) * 100
    df['CENAMA_112'] = df['p64_2a']
    df['CENAMA_113'] = (df['p64_2a'] / df['p64_7a_total']) * 100
    df['CENAMA_115'] = df['p64_3a']
    df['CENAMA_116'] = (df['p64_3a'] / df['p64_7a_total']) * 100
    df['CENAMA_118'] = df['p64_4a']
    df['CENAMA_119'] = (df['p64_4a'] / df['p64_7a_total']) * 100
    df['CENAMA_121'] = df['p64_5a']
    df['CENAMA_122'] = (df['p64_5a'] / df['p64_7a_total']) * 100
    df['CENAMA_124'] = df['p64_5b_total']
    df['CENAMA_126'] = df['p64_1b']
    df['CENAMA_127'] = (df['p64_1b'] / df['p64_5b_total']) * 100
    df['CENAMA_129'] = df['p64_2b']
    df['CENAMA_130'] = (df['p64_2b'] / df['p64_5b_total']) * 100
    df['CENAMA_132'] = df['p64_3b']
    df['CENAMA_133'] = (df['p64_3b'] / df['p64_5b_total']) * 100
    df['CENAMA_135'] = df['p40_1']
    df['CENAMA_137'] = df['p40_3']
    df['CENAMA_138'] = df['p30_1']
    df['CENAMA_141'] = df['p30_2']
    df['CENAMA_144'] = df['p30_3']
    df['CENAMA_147'] = df['p31_1']
    df['CENAMA_150'] = df['p31_2']
    df['CENAMA_153'] = df['p34a']
    df['CENAMA_156'] = df['indicator_formality']
    df['CENAMA_165'] = df['p30_4']
    df['CENAMA_168'] = df['p49_5']
    df['CENAMA_171'] = df['p50']
    df['CENAMA_172'] = df['p51']
    df['CENAMA_173'] = df['p54_1']
    df['CENAMA_176'] = df['p54_2']
    df['CENAMA_179'] = df['p54_3']
    df['CENAMA_182'] = df['p59a_1']
    df['CENAMA_185'] = df['p59a_2']
    df['CENAMA_188'] = df['p59a_3']
    df['CENAMA_191'] = df['p59a_4']
    df['CENAMA_194'] = df['p59a_5']
    df['CENAMA_197'] = df['p59a_6']
    df['CENAMA_200'] = df['p59a_7']
    df['CENAMA_203'] = df['p59a_9']
    df['CENAMA_206'] = df['p60_1']
    df['CENAMA_209'] = df['p60_2']
    df['CENAMA_212'] = df['p60_3']
    df['CENAMA_215'] = df['p60_4']
    df['CENAMA_218'] = df['p60_5']
    df['CENAMA_221'] = df['p60_6']
    
    df = df [[market_id, 'CENAMA_1', 'CENAMA_2', 'CENAMA_3', 'CENAMA_5', 'CENAMA_6', 'CENAMA_7', 'CENAMA_8', 'CENAMA_11', 'CENAMA_14', 
          'CENAMA_17', 'CENAMA_20', 'CENAMA_23', 'CENAMA_26', 'CENAMA_29', 'CENAMA_32', 'CENAMA_35', 'CENAMA_38', 'CENAMA_41', 'CENAMA_42', 
          'CENAMA_43', 'CENAMA_46', 'CENAMA_47', 'CENAMA_48', 'CENAMA_49', 'CENAMA_50', 'CENAMA_51', 'CENAMA_52', 'CENAMA_53', 'CENAMA_54',
          'CENAMA_55', 'CENAMA_56', 'CENAMA_57', 'CENAMA_58', 'CENAMA_59', 'CENAMA_60', 'CENAMA_61', 'CENAMA_62', 'CENAMA_63', 'CENAMA_64',
          'CENAMA_65', 'CENAMA_66', 'CENAMA_67', 'CENAMA_68', 'CENAMA_71', 'CENAMA_73', 'CENAMA_76', 'CENAMA_78', 'CENAMA_81', 'CENAMA_83', 
          'CENAMA_86', 'CENAMA_89', 'CENAMA_92', 'CENAMA_95', 'CENAMA_98', 'CENAMA_101', 'CENAMA_104', 'CENAMA_107', 'CENAMA_109', 'CENAMA_110',
          'CENAMA_112', 'CENAMA_113', 'CENAMA_115', 'CENAMA_116', 'CENAMA_118', 'CENAMA_119', 'CENAMA_121', 'CENAMA_122', 'CENAMA_124', 'CENAMA_126', 
          'CENAMA_127', 'CENAMA_129', 'CENAMA_130', 'CENAMA_132', 'CENAMA_133', 'CENAMA_135', 'CENAMA_137', 'CENAMA_138', 'CENAMA_141', 'CENAMA_144',
          'CENAMA_147', 'CENAMA_150', 'CENAMA_153', 'CENAMA_156', 'CENAMA_165', 'CENAMA_168', 'CENAMA_171', 'CENAMA_172', 'CENAMA_173', 'CENAMA_176', 
          'CENAMA_179', 'CENAMA_182', 'CENAMA_185', 'CENAMA_188', 'CENAMA_191', 'CENAMA_194', 'CENAMA_197', 'CENAMA_200', 'CENAMA_203', 'CENAMA_206', 
          'CENAMA_209', 'CENAMA_212', 'CENAMA_215', 'CENAMA_218', 'CENAMA_221']]

    return df