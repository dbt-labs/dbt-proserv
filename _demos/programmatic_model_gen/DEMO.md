# Programmatic Model Generation Demo
This code demonstrates how you can use command line/terminal
and python to generate many models at one time.

This demo was created using dbt Cloud CLI and Python 3.11.

Python modules used:
+ subprocess: to run a macro using dbt
+ ast: to convert the results of the macro to a Python dictionary
+ pathlib: to create the directory to store the generated files in if it doesn't exist.\

Additionally, this was created for a specific use case to generate 
models using the inc_stream package, but should be flexible enough
to customize to different use cases.

### How to use
For visual learners, [here's a loom video](https://www.loom.com/share/daf635da1f164109bf41989b4c0e13b8?sid=bbc10104-d0c0-4189-aaa8-f4a247707cef)!

1. **Add a macro to the project**
   This macro works similar to code-gen and returns the code content
   of all tables in a particular database/schema, with the option to 
   match table patterns or exclude tables.

   Use the file located at `macros/generate_inc_stream_model_sql.sql` within this folder.

2. **Add a python script to the project**
   The location here is absolutely optional as long as you change the script to 
   work with the file paths you want. This demo puts it at the same level as dbt_project.yml.

   This script will run the macro and get the results, then use
   the results to create and write the files. 
   
   Use the file located at `scripts/create_dbt_inc_stream_model.py` within this folder.
 
3. **Sit back and watch the magic happen**
   After ensuring you've covered customizing the "To Use" points in the script, 
   run the Python script and watch it do all the work!
