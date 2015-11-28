from MDAnalysis.core.distances import distance_array, self_distance_array
from MDAnalysis.analysis.distances import contact_matrix
import numpy as np
import time, os, sys, gc

files=["../md_centered.xtc_95Atoms.np_txt", "../md_prod_12x12_everymicroS_pbcmolcenter.xtc_44784Atoms.np_txt", "../vesicle_1_5M_373_stride1000.xtc_145746Atoms.np_txt"]

results=[]
for f in files:
    #print f
    coord = np.loadtxt(f, dtype='float32')
    start = time.time()
    #distance_array(coord, coord, box=None)
    contact_matrix(coord, returntype="sparse")
    print "ComputeDistanceMDAnalysis, %d, %.2f"%(len(coord), (time.time()-start))
    del coord
    gc.collect()
