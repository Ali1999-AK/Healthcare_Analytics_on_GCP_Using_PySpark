import os
#provides function to work with os and enviroment variables
import pprint as pp

os.environ['envn']="PROD"
os.environ['header']= 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = "USA Prescriber Research Report"
current_path = os.getcwd()
#staging_dim_city = current_path+'\..\staging\dimension_city'

#staging_fact = current_path + '\..\staging\\fact'


staging_dim_city = "PrescPipeline/staging/dimension_city"

staging_fact = "PrescPipeline/staging/fact"

output_fact = "PrescPipeline/output/fact"

output_city= "PrescPipeline/output/dimension_city"
