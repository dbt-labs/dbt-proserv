# You could also use dbtRunner that comes with dbt Core, if not using dbt Cloud
# CLI. This is written to operate using dbt Cloud CLI.

# To use:
# 1. This is for creating multiple dbt models using the inc_stream package 
#    (https://hub.getdbt.com/arnoN7/incr_stream/latest/). Make sure this is
#    included in your packages.yml and you have run dbt deps before you test
#    the models generated.
# 2. Make sure you have the generate_inc_stream_model_sql.sql macro in your project
# 3. Make sure you are at the parent directory of your dbt project and this .py
#    file is at the same level as your dbt_project.yml folder
# 4. Change these parameters if desired:
#    + file_path: The folder that you want the generated files to go in. This 
#                 does not have to exist before you run the .py 
#    + source_config: Make sure this is pointed to the databases and schema
#                     that your sources are located in.
import subprocess
import ast
import pathlib

file_path = 'models/staging/_generated_models/'
pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)

source_config = '''{
    "database_name": "raw_tpch",
    "schema_name": "tpch_sf1"
}'''

response = subprocess.run(
    ['dbt', 'run-operation', 'generate_inc_stream_model_sql', '--args', source_config],
    capture_output = True, # Python >= 3.7 only
    text = True # Python >= 3.7 only
)

start = '[{'
end = '}]'
result_string = start + response.stdout.split(start)[1].split(end)[0] + end
results = ast.literal_eval(result_string)

for result in results:
    model_path = file_path + result["name"]
    model_content = result["sql"]

    with open(model_path, "w") as model:
        model.write(model_content)
    
    print('Generated model ' + model_path + '.')

print('Done.')