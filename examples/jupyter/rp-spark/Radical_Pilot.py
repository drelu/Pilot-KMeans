
# coding: utf-8

# # RADICAL-Pilot Tutorial
# 
# Utilize the examples below to familiarize yourself with RADICAL-Pilot.
# 
# We will:
# * Modify settings (environment variables) if needed
# * Modify the example to print out the hostname of the machine that runs the Pilot
# 
# 
# **Please make sure that you always close the session before terminating the notebook using `session.close()`**
# 
# ## 1 RADICAL-Pilot Setup
# 
# Documentation: http://radicalpilot.readthedocs.org/en/latest/machconf.html#preconfigured-resources
# 
# First, we will import the necessary dependencies and define some helper functions.

# In[11]:

import os, sys
import commands
import radical.pilot as rp
import random
import pandas as pd
import ast
import seaborn as sns

def print_details(detail_object):
    if type(detail_object)==str:
        detail_object = ast.literal_eval(detail_object)
    for i in detail_object:
        detail_object[i]=str(detail_object[i])
    return pd.DataFrame(detail_object.values(), 
             index=detail_object.keys(), 
             columns=["Value"])

os.environ["RADICAL_PILOT_VERBOSE"]="ERROR"
os.environ["RADICAL_SAGA_PTY_VERBOSE"]="DEBUG" 
os.environ["RADICAL_PILOT_DBURL"]="mongodb://localhost:27017/sc15-test000"


# ## 2. Local Pilot Example
# 
# This example shows how to execute a task using a Pilot-Job running on the local machine. In this case, the Pilot-Job is started using **ssh** on the edge node machine of the Hadoop cluster (which runs Jupyterhub - the iPython notebook server).
# 
# ### 2.1 Create a new Session and Pilot-Manager. 

# In[12]:

session = rp.Session()
pmgr = rp.PilotManager(session=session)
umgr = rp.UnitManager (session=session,
                       scheduler=rp.SCHED_ROUND_ROBIN)
print "Session id: %s Pilot Manager: %s" % (session.uid, str(pmgr.as_dict()))


# In[13]:

print_details(umgr.as_dict())


# ### 2.2 Submit Pilot and add to Unit Manager

# In[14]:

pdesc = rp.ComputePilotDescription()
pdesc.resource = "local.localhost_spark_ana"  # NOTE: This is a "label", not a hostname
pdesc.runtime  = 10 # minutes
pdesc.cores    = 2
pdesc.cleanup  = False
pilot = pmgr.submit_pilots(pdesc)
umgr.add_pilots(pilot)





# In[8]:

print_details(pilot.as_dict())


# ### 2.3 Submit Compute Units
# 
# Create a description of the compute unit, which specifies the details of the task to be executed.

# In[76]:

cudesc = rp.ComputeUnitDescription()
cudesc.environment = {'CU_NO': 1}
cudesc.executable  = "/bin/echo"
cudesc.arguments   = ['I am CU number $CU_NO']
cudesc.cores       = 1
print_details(cudesc.as_dict())


# Submit the previously created ComputeUnit descriptions to the PilotManager. This will trigger the selected scheduler (in this case the round-robin scheduler) to start assigning ComputeUnits to the ComputePilots.

# In[78]:

print "Submit Compute Units to Unit Manager ..."
cu_set = umgr.submit_units([cudesc])
print "Waiting for CUs to complete ..."
umgr.wait_units()
print "All CUs completed successfully!"
cu_results = cu_set[0]
details=cu_results.as_dict()


# ---
# The next command will provide the state of the Pilot and other pilot details.

# In[79]:

print_details(details)


# And some more details...

# In[80]:

print_details(details["execution_details"])


# Parse the output of the CU

# In[81]:

print cu_results.stdout.strip()


# ### 2.4 Exercise
# 
# Write a task (i.e., a ComputeUnit) that prints out the hostname of the machine!
# 
# Answer: In the example above, in cudesc.executable replace `/bin/echo` with `hostname`.

# ### 2.5 Performance Analysis
# 
# In the examples below we will show how RADICAL-Pilot can be used for interactive analytics. We will plot and analyze the execution times of a set of ComputeUnits.

# In[82]:

def get_runtime(compute_unit):
    details=compute_unit.as_dict()
    execution_details=details['execution_details']
    state_details=execution_details["statehistory"]
    results = {}
    for i in state_details:
        results[i["state"]]=i["timestamp"]
    start = results["Scheduling"]
    end = results["Done"]
    runtime = end-start
    return runtime


# In[83]:

import random
cudesc_list = []
for i in range(20):
    cudesc = rp.ComputeUnitDescription()
    cudesc.executable  = "/bin/sleep"
    cudesc.environment = {'CU_NO': i}
    cudesc.arguments   = ['%d'%(random.randrange(10))]
    cudesc.cores       = 1
    cudesc_list.append(cudesc)


# In[84]:

cu_set = umgr.submit_units(cudesc_list)


# In[85]:

states = umgr.wait_units()


# In[86]:

runtimes=[]
for compute_unit in cu_set:
    str(compute_unit)
    runtimes.append(get_runtime(compute_unit))


# `/bin/sleep` assigns a random sleep time. We plot the distribution of runtimes of the above 20 ComputeUnits using [Seaborn](http://stanford.edu/~mwaskom/software/seaborn/). See [distplot documentation](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html?highlight=distplot).

# In[87]:

plot=sns.distplot(runtimes, kde=False, axlabel="Runtime")


# ### 2.6 Close and Delete Session 

# In[88]:

session.close()
del session


# ## 3. YARN Pilot Example
# 
# Having submitted multiple jobs using RADICAL-Pilot locally, in this section we will examine how to submit multiple tasks to a YARN cluster using RADICAL-Pilot. Although not of primary importance for this tutorial, it is worth noting that the YARN cluster is a remote resource compared to the submission host (edge node).
# 
# 
# ![Pilot YARN](../figures/radical-pilot-yarn-cu.png)
# 
# ### 3.1 Create a new Session and Pilot-Manager. 
# 

# In[89]:

import getpass
yarn_session = rp.Session()
c = rp.Context('ssh')
c.user_id = getpass.getuser()
yarn_session.add_context(c)
pmgr = rp.PilotManager(session=yarn_session)
umgr = rp.UnitManager(session=yarn_session,
                      scheduler=rp.SCHED_ROUND_ROBIN)
print "Session id: %s Pilot Manager: %s" % (yarn_session.uid, str(pmgr.as_dict()))


# ### 3.2 Submit Pilot and add to Unit Manager
# 
# Note the change in the resource description:
# 
#         pdesc.resource = "yarn.aws-vm"  # NOTE: This is a "label", not a hostname

# In[90]:

pdesc = rp.ComputePilotDescription ()
pdesc.resource = "yarn.aws-vm"  # NOTE: This is a "label", not a hostname
pdesc.runtime  = 30 # minutes
pdesc.cores    = 1
pdesc.cleanup  = False
# submit the pilot.
print "Submitting Compute Pilot to Pilot Manager ..."
pilot = pmgr.submit_pilots(pdesc)
umgr.add_pilots(pilot)


# In[91]:

print_details(pilot.as_dict())


# ### 3.3 Submit Compute Units
# 
# Create a description of the compute unit, which specifies the details on the executed task.

# In[92]:

cudesc = rp.ComputeUnitDescription()
cudesc.environment = {'CU_NO': "1"}
cudesc.executable  = "/bin/echo"
cudesc.arguments   = ['I am CU number $CU_NO']
cudesc.cores       = 1
print_details(cudesc.as_dict())


# Submit the previously created ComputeUnit descriptions to the PilotManager. This will trigger the selected scheduler to start assigning ComputeUnits to the ComputePilots.

# In[93]:

print "Submit Compute Units to Unit Manager ..."
cu_set = umgr.submit_units([cudesc])
print "Waiting for CUs to complete ..."
umgr.wait_units()
print "All CUs completed successfully!"
cu_results = cu_set[0]
details=cu_results.as_dict()


# In[94]:

print_details(details)


# In[63]:

print_details(details["execution_details"])


# In[64]:

print cu_results.stdout.strip()


# ## 4. Close and Delete Session

# In[95]:

yarn_session.close()
del yarn_session

