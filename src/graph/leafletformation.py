#!/usr/bin/env python

"""Simple version of the LeafletFinder algorithm
MDAnalysis.analysis.leaflet.LeafletFinder. Read input topology and
coordinates and write out the number of leaflets and the sizes of the
top 5 ones.

"""
from __future__ import print_function

import numpy as np

import MDAnalysis
import MDAnalysis.core.distances
import MDAnalysis.core.parallel.distances

import networkx as nx

def leafletFinder(atoms, cutoff=16.0, box=None, Nmax=None, adj=None):
    """Basic version of LeafletFinder

    :Arguments:
      * atoms: AtomGroup (eg phosphates)
      * cutoff: treat atoms within *cutoff* Angstrome 
                as adjacent  [16.0]
      * box: unitcell if periodic boundaries should be taken
             into account; ``None`` will ignore PBC
             [None]
      * Nmax: for DEBUGGING: only take the top Nmax x Nmax adjacency matrix;
           set to ``None`` for all. [None]
           
           Use this parameter to test how the implementation behaves
           for larger and larger problem sizes.

           .. warning:: Any Nmax != None will lead to **wrong results**
                        because you arbitrarily throw away data. This is 
                        *only* meant for testing/debugging where you want
                        to find out if the algorithm runs at all.

      * adj: provide adjacency matrix instead of calculating it
             (useful for interactive use to speed up multiple runs
             with different Nmax values... see warning there!)

    :Returns: AtomGroups of leaflets
    """
    if adj is None:
        x = atoms.positions
        # could optimize by having self_distance_array return a full NxN matrix
        # instead of a 1D list (resorting into NxN in python is far too slow!)
        # serial version:
        ##adj = (MDAnalysis.core.distances.distance_array(x, x, box=box) < cutoff)
        # threaded (parallel version):
        adj = (MDAnalysis.core.parallel.distances.distance_array(x, x, box=box) < cutoff)

    # DEBUGGING: with Nmax != None: only take a part of the matrix to see how
    #            how the code behaves for smaller problem sizes
    adjk = adj if Nmax is None else adj[:Nmax, :Nmax] 
    graph = nx.Graph(adjk)
    subgraphs = nx.connected_components(graph)
    indices = [np.sort(g) for g in subgraphs]
    return [atoms[group].residues for group in indices]


    # could optimize by having self_distance_array return a full NxN matrix
    # instead of a 1D list (resorting into NxN in python is far too slow!)

    # d = (MDAnalysis.core.distances.self_distance_array(x, box=box) < cutoff)
    # N = len(x)
    # adj = np.zeros((N, N), dtype=np.bool)
    # for i in xrange(N):
    #     for j in xrange(i+1, N):
    #         k += 1
    #         adj[i,j] = adj[j,i] = d[k]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topology', help="topology file")
    parser.add_argument('trajectory', help="trajectory file")
    parser.add_argument('--cutoff', type=float, default=16.,
                        help="phosphates below the cutoff distance "
                        "CUTOFF Angstrom are considered neighbors [16]")
    parser.add_argument("--selection", default="name P*", 
                        help="MDAnalysis selection string to select atoms "
                        "in the lipid head group ['name P*']")

    #topology = "AnnaDuncan/md_prod_12x12_lastframe.pdb"
    #trajectory = "AnnaDuncan/md_prod_12x12_everymicroS_pbcmolcenter.xtc"

    args = parser.parse_args()

    u = MDAnalysis.Universe(args.topology, args.trajectory)
    phosphates = u.atoms.selectAtoms(args.selection)
    # note that this can select multiple phosphates in one residue such as PIP2
    # which is not really a problem, just increases the problem size a bit
    print("# Input topology/trajectory: {0} {1}".format(args.topology, args.trajectory))
    print("# Head group atom selection: {}".format(args.selection))
    print("# Number of lipids:          {}".format(phosphates.numberOfResidues()))
    print("# Number of phosphates (P):  {}".format(phosphates.numberOfAtoms()))

    print("# [time(ps)] [N_leaflets] [N_lipids for the top 5 leaflets]")
    for ts in u.trajectory:
        L = leafletFinder(phosphates, cutoff=args.cutoff)
        print("{0:9.1f} {1:3d} ".format(u.trajectory.time, len(L)), end='')
        print(" ".join(["{0:5d}".format(g.numberOfResidues()) for g in L[:5]]))

