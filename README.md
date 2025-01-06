# pyDatabases
Database classes based on pandas, scipy, and GAMS. 

## Update in version 0.1.0:
Migrated from old GAMS Python API to the new version (>42.0). Test environment currently uses GAMS 46

## Update in version 0.1.7:
Added new default system_directory to ```gams.GamsWorkspace``` and ```gams.core.numpy.gams2numpy.Gams2Numpy instances```. The default is now to look for location of GAMS installation at "/GAMS/{major gamsapi version}". With Windows as OS and gamsapi version == 46.2.0, it searches for GAMS at "C:/GAMS/46". If this does not work, then it defaults to the main GAMS installation (i.e. it uses ```system_directory = None``` in initialization).

The main effect of this is that with more than one installation of GAMS, we can set up a virtual environment with the corresponding gamsapi and the package should automatically align GAMS installation with gamsapi. 