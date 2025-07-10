"""
Script for fixing incompatibilities between USLCI v1.2025-06.0 and Brightway25 (BW25)

The specific goals of this script are to fix missing data or structural differences that:
    1) causes failure of BW25's apply_strategies() applied to the JSONLDImporter object.
    2) contribute to unlinked edges within the JSONLDImporter object
    3) yield a non-square technosphere matrix (one product flow per process/activity)

List of implemented fixes:
    -


"""

### DEPENDENCIES ###

# Brightway

