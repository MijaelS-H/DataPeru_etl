
COLUMNS_RENAME = {
    'año': 'year',
    'indicador': 'indicator_id',
    'categoría': 'category_id'
}

REPLACE_DICT = {
   'Explotación de minas y cante' : 'Explotación de minas y canteras',
   'Suministro de agua, evacuación de aguas residuales, gestión de desechos y descontaminación' : 'Suministro de agua; evacuación de aguas residuales, gestión de desechos y descontaminación',
   'Comercio al por mayor y al por menor, reparación de vehículos automotores y motocicletas' : 'Comercio al por mayor y al por menor; reparación de vehículos automotores y motocicletas',
   'Actividades profesionales, científicas y técnica' : 'Actividades profesionales, científicas y técnicas',
   'Trabajodores de servicios y vendedores' : 'Trabajadores de servicios y vendedores'
}

INDUSTRY_REPLACE = {'Agricultura, ganadería, silvicultura y pesca': 'A',
 'Explotación de minas y canteras': 'B',
 'Industrias manufactureras': 'C',
 'Suministro de electricidad, gas, vapor y aire acondicionado': 'D',
 'Suministro de agua; evacuación de aguas residuales, gestión de desechos y descontaminación': 'E',
 'Construcción': 'F',
 'Comercio al por mayor y al por menor; reparación de vehículos automotores y motocicletas': 'G',
 'Transporte y almacenamiento': 'H',
 'Actividades de alojamiento y de servicio de comidas': 'I',
 'Información y comunicaciones': 'J',
 'Actividades financieras y de seguros': 'K',
 'Actividades inmobiliarias': 'L',
 'Actividades profesionales, científicas y técnicas': 'M',
 'Actividades de servicios administrativos y de apoyo': 'N',
 'Administración pública y defensa; planes de seguridad social de afiliación obligatoria': 'O',
 'Enseñanza': 'P',
 'Actividades de atención de la salud humana y de asistencia social': 'Q',
 'Actividades artísticas, de entretenimiento y recreativas': 'R',
 'Otras actividades de servicios': 'S',
 'Actividades de los hogares como empleadores; actividades no diferenciadas de los hogares como productores de bienes y servicios para uso propio': 'T',
 'Actividades de organizaciones y órganos extraterritoriales': 'U',
 'No determinado': 'Z'}