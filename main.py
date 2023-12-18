# adaniport_analysis_work.py
from adaniport_dataset_work.adaniport_analysis_work import script1
from reliance_dataset_work.Reliance_analysis_work import script2

script1()
script2()

from dagster import Definitions, load_assets_from_modules
 
from .assets import  adaniport_analysis_work, Reliance_analysis_work
 
Reliance_analysis_work = load_assets_from_modules([Reliance_analysis_work])
adaniport_analysis_work = load_assets_from_modules([adaniport_analysis_work])
 
 
defs = Definitions(
    assets=[*adaniport_analysis_work, *Reliance_analysis_work]
)




